import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from core.openai_analyzer import CommentAnalyzer
from core.ticket_store import TicketStore
from core.ubersmith_client import UbersmithClient

logger = logging.getLogger(__name__)

BUSINESS_TZ = ZoneInfo("America/Puerto_Rico")
ACTION_LABELS = {
    "disconnect": "Desconexión de servicios",
    "reconnect": "Reconexión de servicios",
}
ACTION_SUBJECT_HINTS = {
    "disconnect": ("desconex", "disconnect", "corte"),
    "reconnect": ("reconex", "reconnect"),
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
        self.analysis_ttl = int(os.getenv("CONNEX_ANALYSIS_TTL_SECONDS", "604800"))
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

        lock_ttl = int(os.getenv("CONNEX_LOCK_TTL_SECONDS", "1800"))
        if not self.store.acquire_lock(lock_ttl):
            logger.warning("Hay otro ciclo en curso; se omite este para evitar reprocesar.")
            summary["skipped_locked"] = True
            return summary

        try:
            return self._process_clients_locked(summary, max_clients, client_ids)
        finally:
            self.store.release_lock()

    def _process_clients_locked(self, summary, max_clients, client_ids):
        clients = self._load_clients(client_ids)
        logger.info("Clientes a revisar: %s", len(clients))

        for client_id, client_data in clients.items():
            if max_clients is not None and summary["clients_scanned"] >= max_clients:
                break
            summary["clients_scanned"] += 1
            try:
                result = self._process_one(client_id, client_data)
            except Exception:
                logger.exception("Error procesando cliente %s", client_id)
                summary["errors"] += 1
                continue

            if not result:
                continue
            if result.get("status") == "created":
                summary["tickets_created"] += 1
                summary["analyzed"] += 1
                summary["actions"].append(result)
            elif result.get("status") == "duplicate":
                summary["skipped_duplicate"] += 1
                summary["analyzed"] += 1
            elif result.get("status") == "open_ticket":
                summary["skipped_open_ticket"] += 1
                summary["analyzed"] += 1
            elif result.get("status") == "none":
                summary["skipped_none"] += 1
                summary["analyzed"] += 1

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

    def _process_one(self, client_id, client_data):
        comments = self._list_comments(client_id)
        services = self._list_services(client_id)
        balance = _parse_money(
            client_data.get("balance")
            if client_data.get("balance") not in (None, "")
            else client_data.get("inv_balance")
        )
        balance_bucket = "zero" if balance == 0 else "debt"
        service_status_hash = _stable_hash(
            [(item["service_id"], item.get("status"), item.get("suspended")) for item in services]
        )
        comment_payload = self._comments_for_model(comments)
        comments_hash = _stable_hash(comment_payload)
        analysis_fingerprint = _stable_hash(
            {
                "comments": comments_hash,
                "balance": balance_bucket,
                "services": service_status_hash,
            }
        )

        analysis = self.store.get_cached_analysis(analysis_fingerprint)
        if analysis is None:
            analysis = self.analyzer.analyze(
                client_id,
                comments=comment_payload,
                services=services,
                balance=balance,
                balance_bucket=balance_bucket,
            )
            self.store.cache_analysis(analysis_fingerprint, analysis, self.analysis_ttl)
        else:
            logger.debug("Análisis en caché para cliente %s", client_id)

        action = analysis.get("action") or "none"
        if action == "none":
            return {"status": "none", "client_id": str(client_id)}

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
        payload = self.client.get_paginated(
            "client.comment_list",
            params={"client_id": client_id, "direction": "desc", "order_by": "time"},
            page_size=self.page_size,
        )
        comments = [
            item for item in (payload.get("data") or {}).values() if isinstance(item, dict)
        ]
        comments.sort(key=lambda item: int(item.get("time") or 0), reverse=True)
        return comments[: self.comment_limit]

    def _list_services(self, client_id):
        payload = self.client.get_paginated(
            "client.service_list",
            params={"client_id": client_id, "pack_type_select": 3},
            page_size=self.page_size,
        )
        services = []
        for item in (payload.get("data") or {}).values():
            if not isinstance(item, dict):
                continue
            service_id = item.get("packid") or item.get("service_id")
            if not service_id:
                continue
            services.append(
                {
                    "service_id": str(service_id),
                    "title": item.get("title") or item.get("code") or "",
                    "status": item.get("status") or item.get("servtype") or "",
                    "suspended": str(item.get("suspend_bool") or "0"),
                    "active": str(item.get("active") or ""),
                }
            )
        return services

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

    def _ticket_body(self, client_id, action, reason, balance, services, service_ids, comments):
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
            f"Servicios referidos: {', '.join(service_ids) or 'no especificado'}\n\n"
            f"Servicios del cliente:\n"
            f"{chr(10).join(service_lines) or '- Sin servicios listados'}\n\n"
            f"Comentarios recientes:\n"
            f"{chr(10).join(comment_lines) or '- Sin comentarios'}\n"
        )
