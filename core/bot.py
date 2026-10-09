import hashlib
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from core.openai_analyzer import CommentAnalyzer
from core.ticket_store import TicketStore
from core.ubersmith_client import UbersmithClient

logger = logging.getLogger(__name__)

BUSINESS_TZ = ZoneInfo("America/Puerto_Rico")
CONTRACT_GRACE_DAYS = 61
SERVICE_STATUS = {"1": "active", "2": "pending", "3": "suspended", "4": "cancelled"}
ACTION_LABELS = {
    "disconnect": "Desconexión de servicios",
    "reconnect": "Reconexión de servicios",
}
ACTION_SUBJECT_HINTS = {
    "disconnect": ("desconex", "desconect", "disconnect", "corte"),
    "reconnect": ("reconex", "reconect", "reconnect"),
}


def _parse_money(value):
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _parse_unix_datetime(value):
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(BUSINESS_TZ)


def _stable_hash(payload):
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def apply_business_rules(analysis, invoices, balance, today, has_contract):
    """
    Reglas duras que la IA no puede saltarse. Devuelve (acción, motivo_si_se_anula).

    - Sin contrato: si ya pasó el due_date, toca desconexión. No hay días de gracia.
    - Con contrato: máximo 61 días de atraso desde el due_date más antiguo.
    """
    action = analysis.get("action") or "none"
    if action == "disconnect":
        overdue = [item for item in invoices if item["due_date"] and item["due_date"] < today]
        if not overdue or balance <= 0:
            return "none", "sin_facturas_vencidas"
        if has_contract:
            oldest_due = min(item["due_date"] for item in overdue)
            deadline = oldest_due + timedelta(days=CONTRACT_GRACE_DAYS)
            if today <= deadline:
                return "none", f"prorroga_contrato_hasta_{deadline.isoformat()}"
    elif action == "reconnect":
        if balance > 0 and invoices:
            return "none", "deuda_pendiente"
    return action, None


class ConnexBot:
    def __init__(self, client=None, store=None, analyzer=None):
        self.client = client or UbersmithClient()
        self.store = store or TicketStore()
        self.analyzer = analyzer or CommentAnalyzer()
        self.dry_run = os.getenv("CONNEX_DRY_RUN", "1") != "0"
        self.page_size = int(os.getenv("CONNEX_PAGE_SIZE", self.client.page_size))
        self.comment_limit = int(os.getenv("CONNEX_COMMENT_LIMIT", "25"))
        self.comment_max_chars = int(os.getenv("CONNEX_COMMENT_MAX_CHARS", "400"))
        self.cooldown_hours = float(os.getenv("CONNEX_COOLDOWN_HOURS", "72"))
        self.analysis_ttl = int(os.getenv("CONNEX_ANALYSIS_TTL_SECONDS", "86400"))
        self.lock_ttl = int(os.getenv("CONNEX_LOCK_TTL_SECONDS", "1800"))
        self.default_queue = os.getenv("SUPPORT_QUEUE_ID", "57")
        self.queues = {
            "disconnect": os.getenv("CONNEX_QUEUE_DISCONNECT") or self.default_queue,
            "reconnect": os.getenv("CONNEX_QUEUE_RECONNECT") or self.default_queue,
        }
        self.open_ticket_max_age_days = float(os.getenv("CONNEX_OPEN_TICKET_MAX_AGE_DAYS", "5"))
        self.open_ticket_types = [
            item.strip()
            for item in os.getenv("CONNEX_OPEN_TICKET_TYPES", "Open,On Hold").split(",")
            if item.strip()
        ]
        self.concurrency = max(1, int(os.getenv("CONNEX_CONCURRENCY", "8")))

    def process_clients(self, max_clients=None, client_ids=None):
        """
        1. Lee comentarios y contexto del cliente.
        2. OpenAI decide disconnect / reconnect / none.
        3. Redis evita repetir la misma solicitud reciente.
        4. Ubersmith evita postear si ya hay un ticket abierto reciente en la cola.
        5. Si procede, crea el ticket (POST) y registra IDs + fecha en Redis.
        """
        summary = {
            "clients_scanned": 0,
            "analyzed": 0,
            "skipped_none": 0,
            "skipped_duplicate": 0,
            "skipped_open_ticket": 0,
            "tickets_created": 0,
            "errors": 0,
            "dry_run": self.dry_run,
            "actions": [],
        }

        lock_token = self.store.acquire_lock(self.lock_ttl)
        if not lock_token:
            logger.warning("Hay otro ciclo en curso; se omite este para evitar reprocesar.")
            summary["skipped_locked"] = True
            return summary

        try:
            return self._process_clients_locked(summary, max_clients, client_ids, lock_token)
        finally:
            self.store.release_lock(lock_token)

    def _process_clients_locked(self, summary, max_clients, client_ids, lock_token):
        clients = self._load_clients(client_ids)
        items = list(clients.items())
        if max_clients is not None:
            items = items[: max(0, max_clients)]
        workers = min(self.concurrency, len(items)) or 1
        logger.info("Clientes a revisar: %s (concurrencia=%s)", len(items), workers)

        if not items:
            return summary

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self._process_one, client_id, client_data): client_id
                for client_id, client_data in items
            }
            for future in as_completed(futures):
                client_id = futures[future]
                if not self.store.refresh_lock(lock_token, self.lock_ttl):
                    logger.error("Se perdió el candado del ciclo; se espera a los hilos en curso y se corta.")
                    summary["lock_lost"] = True
                    for pending in futures:
                        pending.cancel()
                    break
                summary["clients_scanned"] += 1
                try:
                    result = future.result()
                except Exception:
                    logger.exception("Error procesando cliente %s", client_id)
                    summary["errors"] += 1
                    continue
                self._record_result(summary, result)

        logger.info(
            "Ciclo terminado: scanned=%s created=%s duplicados=%s abiertos=%s none=%s errors=%s dry_run=%s",
            summary["clients_scanned"],
            summary["tickets_created"],
            summary["skipped_duplicate"],
            summary["skipped_open_ticket"],
            summary["skipped_none"],
            summary["errors"],
            self.dry_run,
        )
        return summary

    @staticmethod
    def _record_result(summary, result):
        if not result:
            return
        status = result.get("status")
        if status == "created":
            summary["tickets_created"] += 1
            summary["analyzed"] += 1
            summary["actions"].append(result)
        elif status == "duplicate":
            summary["skipped_duplicate"] += 1
            summary["analyzed"] += 1
        elif status == "open_ticket":
            summary["skipped_open_ticket"] += 1
            summary["analyzed"] += 1
        elif status == "none":
            summary["skipped_none"] += 1
            summary["analyzed"] += 1

    def _process_one(self, client_id, client_data):
        today = datetime.now(BUSINESS_TZ).date()
        comments = self._list_comments(client_id)
        services = self._list_services(client_id)
        invoices = self._list_unpaid_invoices(client_id, today)
        balance = _parse_money(
            client_data.get("balance")
            if client_data.get("balance") not in (None, "")
            else client_data.get("inv_balance")
        )
        balance_bucket = "zero" if balance <= 0 else "debt"
        service_status_hash = _stable_hash(
            [(item["service_id"], item.get("status"), item.get("suspended")) for item in services]
        )
        comment_payload = self._comments_for_model(comments)
        comments_hash = _stable_hash(comment_payload)
        # El día forma parte de la huella: promesas de pago y prórrogas vencen con el tiempo.
        analysis_fingerprint = _stable_hash(
            {
                "comments": comments_hash,
                "balance": balance_bucket,
                "services": service_status_hash,
                "invoices": [(item["invid"], item["due_date"], item["amount_unpaid"]) for item in invoices],
                "day": today.isoformat(),
            }
        )

        has_contract = False
        for item in services:
            term = str(item.get("contract_term") or "").strip().lower()
            if term and term not in ("no contract", "none", "null", ""):
                has_contract = True
                break

        analysis = self.store.get_cached_analysis(analysis_fingerprint)
        if analysis is None:
            analysis = self.analyzer.analyze(
                client_id,
                comments=comment_payload,
                services=services,
                invoices=invoices,
                balance=balance,
                balance_bucket=balance_bucket,
                today=today,
                has_contract=has_contract,
            )
            self.store.cache_analysis(analysis_fingerprint, analysis, self.analysis_ttl)
        else:
            logger.debug("Análisis en caché para cliente %s", client_id)

        action, override_reason = apply_business_rules(analysis, invoices, balance, today, has_contract)
        if override_reason:
            logger.info(
                "Cliente %s: la IA pidió %s pero las reglas lo anulan (%s)",
                client_id,
                analysis.get("action"),
                override_reason,
            )
        if action == "none":
            return {"status": "none", "client_id": str(client_id), "reason": override_reason}

        service_id = (analysis.get("service_ids") or [None])[0]
        situation_hash = _stable_hash(
            {
                "action": action,
                "service_id": str(service_id or ""),
                "balance": balance_bucket,
                "services": service_status_hash,
            }
        )

        skip, skip_reason = self.store.should_skip(
            client_id,
            action=action,
            situation_hash=situation_hash,
            service_id=service_id,
            cooldown_hours=self.cooldown_hours,
        )
        if skip:
            logger.info("Cliente %s omitido (%s): %s", client_id, action, skip_reason)
            return {
                "status": "duplicate",
                "client_id": str(client_id),
                "type": action,
                "reason": skip_reason,
            }

        queue = self.queues[action]
        open_ticket = self._find_blocking_open_ticket(
            client_id,
            queue=queue,
            action=action,
            service_id=service_id,
        )
        if open_ticket:
            logger.info(
                "Cliente %s omitido (%s): ticket %s abierto en cola %s desde %s",
                client_id,
                action,
                open_ticket.get("ticket_id"),
                queue,
                open_ticket.get("created_at"),
            )
            return {
                "status": "open_ticket",
                "client_id": str(client_id),
                "type": action,
                "ticket_id": open_ticket.get("ticket_id"),
                "reason": (
                    f"ticket_abierto ticket_id={open_ticket.get('ticket_id')} "
                    f"cola={queue} desde={open_ticket.get('created_at')}"
                ),
            }

        subject = f"{ACTION_LABELS[action]} - Cliente {client_id}"
        body = self._ticket_body(
            client_id=client_id,
            action=action,
            reason=analysis.get("reason"),
            balance=balance,
            services=services,
            service_ids=analysis.get("service_ids") or [],
            comments=comment_payload[:5],
            invoices=invoices,
            has_contract=has_contract,
        )

        if self.dry_run:
            ticket_id = None
            logger.info(
                "Dry-run: no se crea ticket queue=%s client=%s action=%s",
                queue,
                client_id,
                action,
            )
        else:
            ticket_id = self.client.submit_ticket(
                client_id=client_id,
                subject=subject,
                body=body,
                queue=queue,
                service_id=service_id,
            )
            logger.info(
                "Ticket creado id=%s queue=%s client=%s action=%s",
                ticket_id,
                queue,
                client_id,
                action,
            )
            self.store.record_request(
                {
                    "client_id": str(client_id),
                    "ticket_id": ticket_id,
                    "action": action,
                    "service_id": str(service_id or ""),
                    "queue": str(queue),
                    "reason": analysis.get("reason"),
                    "situation_hash": situation_hash,
                    "comments_hash": comments_hash,
                    "balance_bucket": balance_bucket,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        return {
            "status": "created",
            "type": action,
            "client_id": str(client_id),
            "ticket_id": ticket_id,
            "queue": str(queue),
            "service_id": str(service_id or ""),
            "reason": analysis.get("reason"),
            "confidence": analysis.get("confidence"),
            "dry_run": self.dry_run,
        }

    def _load_clients(self, client_ids):
        if not client_ids:
            return self._list_active_clients()

        clients = {}
        for client_id in client_ids:
            detail = self.client.get("client.get", params={"client_id": client_id})
            clients[str(client_id)] = detail.get("data") or {"clientid": client_id}
        return clients

    def _list_active_clients(self):
        payload = self.client.get_paginated(
            "client.list",
            params={"inactive": 0, "active": 1},
            page_size=self.page_size,
        )
        return payload.get("data") or {}

    def _list_comments(self, client_id):
        payload = self.client.get(
            "client.comment_list",
            params={
                "client_id": client_id,
                "direction": "desc",
                "order_by": "time",
                "limit": self.comment_limit,
            },
        )
        comments = [
            item
            for item in self.client._normalize_data(payload.get("data")).values()
            if isinstance(item, dict)
        ]
        comments.sort(key=lambda item: int(item.get("time") or 0), reverse=True)
        return comments[: self.comment_limit]

    def _list_services(self, client_id):
        # pack_type_select=4: activos, pendientes y suspendidos que no han terminado.
        payload = self.client.get_paginated(
            "client.service_list",
            params={"client_id": client_id, "pack_type_select": 4, "metadata": 1},
            page_size=self.page_size,
        )
        services = []
        for item in (payload.get("data") or {}).values():
            if not isinstance(item, dict):
                continue
            service_id = item.get("packid") or item.get("service_id")
            if not service_id:
                continue
                
            # Ignorar cargos de una sola vez (como reconexiones o facturas sueltas)
            if str(item.get("period") or "0") == "0":
                continue
                
            active_code = str(item.get("active") or "")
            
            metadata = item.get("metadata") or {}
            contract_term = metadata.get("contract_term")
            if contract_term is None:
                contract_term = item.get("contract_term")
                
            services.append(
                {
                    "service_id": str(service_id),
                    "title": (item.get("title") or item.get("code") or "").strip(),
                    "status": SERVICE_STATUS.get(active_code, active_code),
                    "suspended": str(item.get("suspend_bool") or "0"),
                    "contract_term": str(contract_term) if contract_term is not None else None,
                }
            )
        return services

    def _list_unpaid_invoices(self, client_id, today):
        payload = self.client.get_paginated(
            "client.invoice_list",
            params={"client_id": client_id, "paid": 0, "order_by": "due", "direction": "asc"},
            page_size=self.page_size,
        )
        invoices = []
        for item in (payload.get("data") or {}).values():
            if not isinstance(item, dict) or str(item.get("paid", "0")) != "0":
                continue
            due_dt = _parse_unix_datetime(item.get("due"))
            due_day = due_dt.date() if due_dt else None
            invid = str(item.get("invid") or "")
            
            service_ids = []
            if invid:
                try:
                    inv_detail = self.client.get("client.invoice_get", {"invoice_id": invid})
                    data = inv_detail.get("data") or {}
                    packs = data.get("current_packs") or {}
                    for pack in packs.values():
                        packid = pack.get("packid")
                        if packid:
                            service_ids.append(str(packid))
                except Exception as exc:
                    logger.warning("No se pudo obtener detalle de factura %s: %s", invid, exc)

            invoices.append(
                {
                    "invid": invid,
                    "amount_unpaid": str(_parse_money(item.get("amount_unpaid") or item.get("amount"))),
                    "due_date": due_day,
                    "days_overdue": max(0, (today - due_day).days) if due_day else 0,
                    "service_ids": service_ids,
                }
            )
        invoices.sort(key=lambda item: item["due_date"] or date.max)
        return invoices

    def _find_blocking_open_ticket(self, client_id, queue, action, service_id):
        """
        Si ya hay un ticket abierto/en espera en la cola, del mismo cliente,
        creado hace menos de N días y de la misma acción, no se postea otro.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.open_ticket_max_age_days)
        tickets = self._list_open_queue_tickets(client_id, queue, since=cutoff)
        wanted_service = str(service_id or "")

        for ticket in tickets:
            created_at = _parse_unix_datetime(ticket.get("timestamp"))
            if created_at is None:
                continue
            if created_at.astimezone(timezone.utc) < cutoff:
                continue
            if not self._ticket_matches_action(ticket, action):
                continue
            ticket_service = str(ticket.get("service_id") or "")
            if wanted_service and ticket_service and ticket_service not in {"0", wanted_service}:
                continue
            return {
                "ticket_id": str(ticket.get("ticket_id") or ticket.get("id") or ""),
                "subject": ticket.get("subject") or "",
                "created_at": created_at.isoformat(),
                "type": ticket.get("type") or ticket.get("type_name") or "",
            }
        return None

    def _list_open_queue_tickets(self, client_id, queue, since):
        tickets = []
        seen = set()
        since_ts = int(since.timestamp())
        for ticket_type in self.open_ticket_types:
            payload = self.client.get_paginated(
                "support.ticket_list",
                params={
                    "client_id": client_id,
                    "queue": queue,
                    "type": ticket_type,
                    "begin": since_ts,
                    "internal_ticket": 2,
                    "order_by": "timestamp",
                    "direction": "desc",
                },
                page_size=self.page_size,
            )
            for item in (payload.get("data") or {}).values():
                if not isinstance(item, dict):
                    continue
                ticket_id = str(item.get("ticket_id") or item.get("id") or "")
                if ticket_id and ticket_id in seen:
                    continue
                if ticket_id:
                    seen.add(ticket_id)
                tickets.append(item)
        return tickets

    @staticmethod
    def _ticket_matches_action(ticket, action):
        subject = (ticket.get("subject") or "").strip().lower()
        if not subject:
            return True
        own_hints = ACTION_SUBJECT_HINTS.get(action, ())
        other_hints = [
            hint
            for other_action, hints in ACTION_SUBJECT_HINTS.items()
            if other_action != action
            for hint in hints
        ]
        mentions_own = any(hint in subject for hint in own_hints)
        mentions_other = any(hint in subject for hint in other_hints)
        if mentions_other and not mentions_own:
            return False
        return True

    def _comments_for_model(self, comments):
        prepared = []
        for comment in comments:
            text = (comment.get("comment") or "").strip()
            if not text:
                continue
            comment_dt = _parse_unix_datetime(comment.get("time"))
            prepared.append(
                {
                    "id": str(comment.get("comment_id") or comment.get("id") or ""),
                    "time": comment_dt.isoformat() if comment_dt else None,
                    "text": text[: self.comment_max_chars],
                }
            )
        return prepared

    def _ticket_body(
        self, client_id, action, reason, balance, services, service_ids, comments, invoices, has_contract
    ):
        invoice_lines = [
            f"- Factura {item['invid']}: vence {item['due_date'] or 'sin fecha'}, "
            f"impago {item['amount_unpaid']}, {item['days_overdue']} días de atraso "
            f"(Servicios: {', '.join(item.get('service_ids') or []) or 'desconocido'})"
            for item in invoices
        ]
        service_lines = []
        selected = set(service_ids)
        for item in services:
            mark = " [referido]" if item["service_id"] in selected else ""
            service_lines.append(
                f"- Servicio {item['service_id']}: {item['title']} "
                f"(status={item['status']}, suspend={item['suspended']}){mark}"
            )
        comment_lines = []
        for comment in comments:
            comment_lines.append(
                f"- {comment.get('time') or 'sin fecha'}: {comment.get('text')}"
            )
        return (
            f"Solicitud automática de {ACTION_LABELS[action].lower()}.\n\n"
            f"Cliente: {client_id}\n"
            f"Acción: {action}\n"
            f"Motivo: {reason}\n"
            f"Balance: {balance}\n"
            f"Contrato (según comentarios): {'sí' if has_contract else 'no'}\n"
            f"Servicios referidos: {', '.join(service_ids) or 'no especificado'}\n\n"
            f"Facturas impagas:\n"
            f"{chr(10).join(invoice_lines) or '- Sin facturas impagas'}\n\n"
            f"Servicios del cliente:\n"
            f"{chr(10).join(service_lines) or '- Sin servicios listados'}\n\n"
            f"Comentarios recientes:\n"
            f"{chr(10).join(comment_lines) or '- Sin comentarios'}\n"
        )
