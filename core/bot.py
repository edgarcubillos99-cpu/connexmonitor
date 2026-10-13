import hashlib
import json
import logging
import os
import re
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from core.openai_analyzer import CommentAnalyzer
from core.ticket_store import (
    STATE_ACTIVE,
    STATE_DISCONNECT_REQUESTED,
    STATE_DISCONNECTED,
    STATE_RECONNECT_REQUESTED,
    TicketStore,
)
from core.ubersmith_client import UbersmithAPIError, UbersmithClient

logger = logging.getLogger(__name__)

BUSINESS_TZ = ZoneInfo("America/Puerto_Rico")
OVERDUE_INVOICES_WITH_CONTRACT = 2
OVERDUE_INVOICES_WITHOUT_CONTRACT = 1
# Leftovers crónicos: más de N facturas vencidas del mismo circuito no se cortan.
MAX_OVERDUE_INVOICES = 5
# Campo "active" del servicio en Ubersmith: 1 activo, 2 pendiente, 3 suspendido, 4 cancelado.
EVALUABLE_SERVICE_CODES = {"1", "2", "3"}
SUSPENDED_SERVICE_CODE = "3"
CONTRACT_TERM_RE = re.compile(r"(?i)\b(\d+)\s*(?:years?|yrs?|años?|months?|mes(?:es)?)\b")
NO_CONTRACT_VALUES = {"", "none", "null", "n/a", "0", "no contract", "sin contrato"}
FIBERX_PROVIDERS = {"fiberx", "fiber x", "fibra"}
OSNET_PROVIDERS = {"osnet", "osn"}
FIBERX_TITLE_RE = re.compile(r"(?i)\bfiberx\b|\bfiber\b|\bfibra\b")
FIBERX_CODE_RE = re.compile(r"(?i)^(fiber|fdata|fbus|fres|fx)")
# Estado del tipo de ticket en Ubersmith: 1 abierto, 2 en espera, 3 cerrado, 4 borrado.
OPEN_TICKET_STATUSES = {"1", "2"}
CLOSED_TICKET_STATUS = "3"
DELETED_TICKET_STATUS = "4"
ACTION_LABELS = {
    "disconnect": "Desconexión de servicios",
    "reconnect": "Reconexión de servicios",
}
ACTION_SUBJECT_HINTS = {
    "disconnect": ("desconex", "desconect", "disconnect", "corte"),
    "reconnect": ("reconex", "reconect", "reconnect"),
}
REQUESTED_STATE = {
    "disconnect": STATE_DISCONNECT_REQUESTED,
    "reconnect": STATE_RECONNECT_REQUESTED,
}
REQUESTED_ACTION = {state: action for action, state in REQUESTED_STATE.items()}
# Transición cuando el ticket pedido termina: (cerrado completo, cerrado incompleto o borrado).
TICKET_OUTCOME = {
    STATE_DISCONNECT_REQUESTED: (STATE_DISCONNECTED, STATE_ACTIVE),
    STATE_RECONNECT_REQUESTED: (STATE_ACTIVE, STATE_DISCONNECTED),
}


def _parse_money(value):
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None


def invoice_line_is_unpaid(line):
    """
    Una factura impaga lista todos los servicios del periodo, también los ya cubiertos.
    Solo cuenta la línea si Ubersmith le dejó un saldo (unpaid > 0) y no está cancelada.
    """
    if str(line.get("canceled") or "0") not in {"0", ""}:
        return False
    unpaid = _parse_money(line.get("unpaid"))
    if unpaid is not None:
        return unpaid > 0
    value = _parse_money(line.get("value") or line.get("cost"))
    paid = _parse_money(line.get("paid")) or Decimal("0")
    return value is not None and paid < value


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


def _as_items(value):
    if isinstance(value, dict):
        return [item for item in value.values() if isinstance(item, dict)]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def load_ignored_service_ids(path=None):
    """Lee ignored_services.json: lista o {"service_ids": [...]}."""
    raw_path = path or os.getenv("CONNEX_IGNORED_SERVICES_PATH", "ignored_services.json")
    file_path = Path(raw_path)
    if not file_path.is_absolute():
        file_path = Path(__file__).resolve().parent.parent / file_path
    if not file_path.is_file():
        return set()
    payload = json.loads(file_path.read_text())
    if isinstance(payload, dict):
        ids = payload.get("service_ids") or []
    elif isinstance(payload, list):
        ids = payload
    else:
        return set()
    return {str(item).strip() for item in ids if str(item).strip()}


def is_ignored_service(service, ignored_ids):
    return str(service.get("service_id") or "") in ignored_ids


def is_fiberx_service(service):
    """
    FiberX y OSNet conviven en el mismo Ubersmith.
    Título o código con Fiber/FiberX/Fibra se excluye siempre, aunque
    metadata.provider diga OSNET (pasa con planes tipo Fiber Internet Circuit).
    Si no hay pista en el nombre, el provider decide.
    """
    title = service.get("title") or ""
    code = service.get("code") or ""
    if FIBERX_TITLE_RE.search(title) or FIBERX_CODE_RE.search(code):
        return True
    provider = str(service.get("provider") or "").strip().lower()
    if provider in FIBERX_PROVIDERS:
        return True
    if provider in OSNET_PROVIDERS:
        return False
    return False


def contract_term_status(value):
    """True si el término indica contrato, False si dice que no, None si no se reconoce."""
    text = str(value or "").strip()
    if text.lower() in NO_CONTRACT_VALUES or "no contract" in text.lower():
        return False
    match = CONTRACT_TERM_RE.search(text)
    if match and int(match.group(1)) > 0:
        return True
    return None


def circuit_state(service, circuits):
    record = circuits.get(service["service_id"])
    if record:
        return record.get("state") or STATE_ACTIVE
    if service.get("status_code") == SUSPENDED_SERVICE_CODE:
        return STATE_DISCONNECTED
    return STATE_ACTIVE


def decide_actions(services, circuits, overdue_by_service, has_contract, max_overdue=MAX_OVERDUE_INVOICES):
    """
    Reglas por circuito:
    - activo con >= 2 facturas vencidas (con contrato) o >= 1 (sin contrato): desconectar.
    - más de max_overdue vencidas: leftover crónico, no se toca.
    - desconectado sin facturas vencidas de ese servicio: reconectar.
    - con una solicitud en curso: no se hace nada hasta que el ticket termine.
    """
    threshold = OVERDUE_INVOICES_WITH_CONTRACT if has_contract else OVERDUE_INVOICES_WITHOUT_CONTRACT
    decisions = []
    for service in services:
        service_id = service["service_id"]
        state = circuit_state(service, circuits)
        overdue = len(overdue_by_service.get(service_id, []))
        if max_overdue is not None and overdue > max_overdue:
            continue
        if state == STATE_ACTIVE and overdue >= threshold:
            decisions.append(
                {
                    "service_id": service_id,
                    "action": "disconnect",
                    "reason": (
                        f"{overdue} facturas vencidas "
                        f"({'con' if has_contract else 'sin'} contrato, límite {threshold})"
                    ),
                }
            )
        elif state == STATE_DISCONNECTED and overdue == 0:
            decisions.append(
                {
                    "service_id": service_id,
                    "action": "reconnect",
                    "reason": "sin facturas vencidas del servicio",
                }
            )
    return decisions


def group_overdue_by_service(invoices, service_ids, today):
    """Asigna cada factura vencida a sus circuitos; lo que no es de ningún circuito queda aparte."""
    by_service = {}
    unattributed = []
    for invoice in invoices:
        if not invoice["due_date"] or invoice["due_date"] >= today:
            continue
        targets = set()
        for packid, parentpack in invoice.get("packs") or []:
            if packid in service_ids:
                targets.add(packid)
            elif parentpack in service_ids:
                targets.add(parentpack)
        if not targets:
            unattributed.append(invoice)
        for service_id in targets:
            by_service.setdefault(service_id, []).append(invoice)
    return by_service, unattributed


class ConnexBot:
    def __init__(self, client=None, store=None, analyzer=None):
        self.client = client or UbersmithClient()
        self.store = store or TicketStore()
        self.analyzer = analyzer or CommentAnalyzer()
        self.dry_run = os.getenv("CONNEX_DRY_RUN", "1") != "0"
        self.page_size = int(os.getenv("CONNEX_PAGE_SIZE", self.client.page_size))
        self.comment_limit = int(os.getenv("CONNEX_COMMENT_LIMIT", "25"))
        self.comment_max_chars = int(os.getenv("CONNEX_COMMENT_MAX_CHARS", "400"))
        self.analysis_ttl = int(os.getenv("CONNEX_ANALYSIS_TTL_SECONDS", "86400"))
        self.lock_ttl = int(os.getenv("CONNEX_LOCK_TTL_SECONDS", "1800"))
        self.default_queue = os.getenv("SUPPORT_QUEUE_ID", "57")
        self.queues = {
            "disconnect": os.getenv("CONNEX_QUEUE_DISCONNECT") or self.default_queue,
            "reconnect": os.getenv("CONNEX_QUEUE_RECONNECT") or self.default_queue,
        }
        self.open_ticket_max_age_days = float(os.getenv("CONNEX_OPEN_TICKET_MAX_AGE_DAYS", "5"))
        self.max_overdue_invoices = int(os.getenv("CONNEX_MAX_OVERDUE_INVOICES", str(MAX_OVERDUE_INVOICES)))
        self.concurrency = max(1, int(os.getenv("CONNEX_CONCURRENCY", "8")))
        self.ignored_service_ids = load_ignored_service_ids()
        self._ticket_types = {}
        self._ticket_types_lock = threading.Lock()
        if self.ignored_service_ids:
            logger.info("Servicios en lista de ignore: %s", len(self.ignored_service_ids))

    def process_clients(self, max_clients=None, client_ids=None):
        """
        Por cada cliente:
        1. Actualiza el estado de sus circuitos según los tickets pedidos antes.
        2. Reglas en Python deciden desconectar o reconectar cada circuito.
        3. OpenAI solo revisa prórrogas antes de un corte.
        4. No se postea si ya hay un ticket abierto reciente del circuito en la cola.
        5. Se crea el ticket y el circuito queda "solicitado" en Redis hasta que se cierre.
        """
        summary = {
            "clients_scanned": 0,
            "clients_without_debt": 0,
            "services_overdue": 0,
            "disconnect_candidates": 0,
            "reconnect_candidates": 0,
            "openai_calls": 0,
            "openai_cache_hits": 0,
            "disconnects_blocked_by_ai": 0,
            "tickets_created": 0,
            "skipped_open_ticket": 0,
            "skipped_extension": 0,
            "skipped_recently_failed": 0,
            "circuits_updated": 0,
            "errors": 0,
            "dry_run": self.dry_run,
            "actions": [],
            "disconnect_report": [],
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
                except UbersmithAPIError as exc:
                    logger.error("Cliente %s omitido por error de Ubersmith: %s", client_id, exc)
                    summary["errors"] += 1
                    continue
                except Exception:
                    logger.exception("Error procesando cliente %s", client_id)
                    summary["errors"] += 1
                    continue
                self._record_result(summary, result)

        logger.info(
            "Ciclo terminado: scanned=%s mora=%s corte=%s reconexion=%s ia=%s "
            "ia_rechazo=%s created=%s errors=%s dry_run=%s",
            summary["clients_scanned"],
            summary["services_overdue"],
            summary["disconnect_candidates"],
            summary["reconnect_candidates"],
            summary["openai_calls"],
            summary["disconnects_blocked_by_ai"],
            summary["tickets_created"],
            summary["errors"],
            self.dry_run,
        )
        return summary

    @staticmethod
    def _record_result(summary, result):
        if result.get("without_debt"):
            summary["clients_without_debt"] += 1
        for key in (
            "circuits_updated",
            "services_overdue",
            "disconnect_candidates",
            "reconnect_candidates",
            "openai_calls",
            "openai_cache_hits",
            "disconnects_blocked_by_ai",
        ):
            summary[key] += result.get(key, 0)
        summary["disconnect_report"].extend(result.get("disconnect_report") or [])
        for event in result.get("events", []):
            status = event.get("status")
            if status == "created":
                summary["tickets_created"] += 1
                summary["actions"].append(event)
            elif status == "open_ticket":
                summary["skipped_open_ticket"] += 1
            elif status == "extension":
                summary["skipped_extension"] += 1
            elif status == "recently_failed":
                summary["skipped_recently_failed"] += 1

    def _process_one(self, client_id, client_data):
        client_id = str(client_id)
        result = {
            "client_id": client_id,
            "events": [],
            "circuits_updated": 0,
            "services_overdue": 0,
            "disconnect_candidates": 0,
            "reconnect_candidates": 0,
            "openai_calls": 0,
            "openai_cache_hits": 0,
            "disconnects_blocked_by_ai": 0,
            "disconnect_report": [],
        }
        balance = self._client_balance(client_data)

        # Sin deuda y sin circuitos cortados o pedidos: no hay nada que decidir.
        if balance is not None and balance <= 0 and not self.store.client_has_circuits(client_id):
            result["without_debt"] = True
            return result

        today = datetime.now(BUSINESS_TZ).date()
        circuits = self.store.get_client_circuits(client_id)
        invoices = self._list_overdue_invoices(client_id, today)
        # Debe, pero nada vencido todavía y ningún circuito que vigilar.
        if not invoices and not circuits:
            return result

        recurring, services = self._list_services(client_id)
        has_contract = self._client_has_contract(client_id, recurring)
        circuits, updated = self._sync_circuits(client_id, circuits, services)
        result["circuits_updated"] = updated

        service_ids = {service["service_id"] for service in services}
        overdue_by_service, unattributed = group_overdue_by_service(invoices, service_ids, today)
        result["services_overdue"] = len(overdue_by_service)
        decisions = decide_actions(
            services,
            circuits,
            overdue_by_service,
            has_contract,
            max_overdue=self.max_overdue_invoices,
        )
        disconnects = [item for item in decisions if item["action"] == "disconnect"]
        reconnects = [item for item in decisions if item["action"] == "reconnect"]
        result["disconnect_candidates"] = len(disconnects)
        result["reconnect_candidates"] = len(reconnects)
        titles = {service["service_id"]: service.get("title") or "" for service in services}
        if not decisions:
            return result

        comment_payload = []
        blocked_by_ai = False
        if disconnects:
            comment_payload = self._comments_for_model(self._list_comments(client_id))
            extension, ia_source = self._check_extension(client_id, comment_payload, today)
            if ia_source == "openai":
                result["openai_calls"] = 1
            elif ia_source == "cache":
                result["openai_cache_hits"] = 1
            if extension:
                blocked_by_ai = True
                logger.info("Cliente %s: corte detenido por prórroga: %s", client_id, extension)
                result["disconnects_blocked_by_ai"] = len(disconnects)
                for item in disconnects:
                    result["events"].append(
                        {
                            "status": "extension",
                            "client_id": client_id,
                            "service_ids": [item["service_id"]],
                            "reason": extension,
                        }
                    )
                decisions = reconnects
        result["disconnect_report"] = [
            {
                "client_id": client_id,
                "service_id": item["service_id"],
                "title": titles.get(item["service_id"], ""),
                "reason": item["reason"],
                "contrato": "si" if has_contract else "no",
                "rechazado_por_ia": "si" if blocked_by_ai else "no",
            }
            for item in disconnects
        ]

        services_by_id = {service["service_id"]: service for service in services}
        for action in ("disconnect", "reconnect"):
            group = [item for item in decisions if item["action"] == action]
            if not group:
                continue
            invoices = {}
            for item in group:
                for invoice in overdue_by_service.get(item["service_id"], []):
                    invoices[invoice["invid"]] = invoice
            for invoice in unattributed:
                invoices[invoice["invid"]] = invoice
            result["events"].extend(
                self._request_action(
                    client_id=client_id,
                    action=action,
                    decisions=group,
                    services_by_id=services_by_id,
                    circuits=circuits,
                    balance=balance,
                    invoices=sorted(invoices.values(), key=lambda item: item["due_date"]),
                    comments=comment_payload[:5],
                    has_contract=has_contract,
                )
            )
        return result

    def _request_action(
        self, client_id, action, decisions, services_by_id, circuits, balance, invoices, comments, has_contract
    ):
        """Un solo ticket por cliente y acción, con todos los servicios afectados."""
        queue = self.queues[action]
        events = []

        pending = []
        for decision in decisions:
            failed_ticket = self.store.recently_failed(decision["service_id"], action)
            if failed_ticket is None:
                pending.append(decision)
                continue
            logger.info(
                "Cliente %s servicio %s omitido (%s): el ticket %s se cerró sin completarse hace poco",
                client_id,
                decision["service_id"],
                action,
                failed_ticket,
            )
            events.append(
                {
                    "status": "recently_failed",
                    "client_id": client_id,
                    "service_ids": [decision["service_id"]],
                    "type": action,
                    "ticket_id": failed_ticket,
                }
            )
        if not pending:
            return events

        service_ids = [decision["service_id"] for decision in pending]
        single_service = service_ids[0] if len(service_ids) == 1 else None

        open_ticket = self._find_blocking_open_ticket(
            client_id, queue=queue, action=action, service_id=single_service
        )
        if open_ticket:
            logger.info(
                "Cliente %s omitido (%s, %s servicios): ticket %s abierto en cola %s desde %s",
                client_id,
                action,
                len(service_ids),
                open_ticket.get("ticket_id"),
                queue,
                open_ticket.get("created_at"),
            )
            events.append(
                {
                    "status": "open_ticket",
                    "client_id": client_id,
                    "service_ids": service_ids,
                    "type": action,
                    "ticket_id": open_ticket.get("ticket_id"),
                }
            )
            return events

        if single_service:
            subject = f"{ACTION_LABELS[action]} - Cliente {client_id} - Servicio {single_service}"
        else:
            subject = f"{ACTION_LABELS[action]} - Cliente {client_id} - {len(service_ids)} servicios"
        body = self._ticket_body(
            client_id=client_id,
            action=action,
            decisions=pending,
            balance=balance,
            services=list(services_by_id.values()),
            comments=comments,
            invoices=invoices,
            has_contract=has_contract,
        )

        ticket_id = None
        if self.dry_run:
            logger.info(
                "Dry-run: no se crea ticket queue=%s client=%s action=%s servicios=%s",
                queue,
                client_id,
                action,
                len(service_ids),
            )
        else:
            ticket_id = self.client.submit_ticket(
                client_id=client_id,
                subject=subject,
                body=body,
                queue=queue,
                service_id=single_service,
            )
            if not ticket_id:
                raise UbersmithAPIError(f"support.ticket_submit no devolvió ticket_id para cliente {client_id}")
            logger.info(
                "Ticket creado id=%s queue=%s client=%s action=%s servicios=%s",
                ticket_id,
                queue,
                client_id,
                action,
                ",".join(service_ids),
            )
            requested_at = datetime.now(timezone.utc).isoformat()
            for decision in pending:
                service = services_by_id[decision["service_id"]]
                self.store.set_circuit(
                    client_id,
                    decision["service_id"],
                    REQUESTED_STATE[action],
                    cid=service.get("cid"),
                    ticket_id=ticket_id,
                    queue=queue,
                    previous_state=circuit_state(service, circuits),
                    reason=decision["reason"],
                    requested_at=requested_at,
                )
            self.store.record_request(
                {
                    "client_id": client_id,
                    "service_ids": service_ids,
                    "ticket_id": ticket_id,
                    "action": action,
                    "queue": str(queue),
                    "created_at": requested_at,
                }
            )

        events.append(
            {
                "status": "created",
                "type": action,
                "client_id": client_id,
                "service_ids": service_ids,
                "ticket_id": ticket_id,
                "queue": str(queue),
                "reason": pending[0]["reason"] if single_service else f"{len(service_ids)} servicios",
                "dry_run": self.dry_run,
            }
        )
        return events

    def _check_extension(self, client_id, comment_payload, today):
        """Devuelve (motivo o None, origen). None = el corte procede."""
        if not comment_payload:
            return None, "no_comments"
        fingerprint = _stable_hash({"comments": comment_payload, "day": today.isoformat()})
        analysis = self.store.get_cached_analysis(fingerprint)
        source = "cache"
        if analysis is None:
            analysis = self.analyzer.check_extensions(client_id, comments=comment_payload, today=today)
            self.store.cache_analysis(fingerprint, analysis, self.analysis_ttl)
            source = "openai"
        if analysis.get("action") == "disconnect":
            return None, source
        return analysis.get("reason") or "prórroga detectada", source

    def _sync_circuits(self, client_id, circuits, services):
        """
        Mueve los circuitos "solicitados" según el ticket: cerrado = hecho;
        cerrado incompleto o borrado = vuelve al estado anterior.
        """
        service_ids = {service["service_id"] for service in services}
        current = {}
        updated = 0
        outcomes = {}
        for service_id, record in circuits.items():
            if service_id not in service_ids:
                # El servicio se canceló o terminó: ya no es un circuito a vigilar.
                self.store.delete_circuit(client_id, service_id)
                updated += 1
                continue
            state = record.get("state")
            if state not in TICKET_OUTCOME or not record.get("ticket_id"):
                current[service_id] = record
                continue
            # Varios circuitos comparten el mismo ticket; se lee una sola vez.
            if record["ticket_id"] not in outcomes:
                outcomes[record["ticket_id"]] = self._ticket_outcome(
                    record["ticket_id"], record.get("queue") or self.default_queue
                )
            outcome = outcomes[record["ticket_id"]]
            if outcome is None:
                current[service_id] = record
                continue
            done_state, failed_state = TICKET_OUTCOME[state]
            new_state = done_state if outcome == "done" else failed_state
            logger.info(
                "Cliente %s servicio %s: ticket %s %s; %s -> %s",
                client_id,
                service_id,
                record["ticket_id"],
                outcome,
                state,
                new_state,
            )
            fields = {
                key: record.get(key)
                for key in ("cid", "ticket_id", "queue", "reason", "requested_at")
                if record.get(key)
            }
            self.store.set_circuit(client_id, service_id, new_state, **fields)
            if outcome == "failed":
                # Sin esta pausa, un "Closed Incomplete" haría pedir lo mismo en cada ciclo.
                self.store.mark_failed(
                    service_id,
                    REQUESTED_ACTION[state],
                    record["ticket_id"],
                    self.open_ticket_max_age_days * 86400,
                )
            updated += 1
            if new_state != STATE_ACTIVE:
                current[service_id] = {**record, "state": new_state}
        return current, updated

    def _ticket_outcome(self, ticket_id, queue):
        """'done', 'failed' o None si el ticket sigue en proceso o no se pudo leer."""
        try:
            ticket = self.client.get("support.ticket_get", params={"ticket_id": ticket_id}).get("data") or {}
        except UbersmithAPIError as exc:
            logger.warning("No se pudo leer el ticket %s: %s", ticket_id, exc)
            return None
        type_id = str(ticket.get("type_id") or ticket.get("type") or "")
        type_name = str(ticket.get("type_name") or "")
        known = self._queue_ticket_types(queue).get(type_id)
        if known:
            type_name = known["name"] or type_name
            status = known["status"]
        else:
            status = {"closed": CLOSED_TICKET_STATUS, "deleted": DELETED_TICKET_STATUS}.get(type_name.lower(), "")
        if status == CLOSED_TICKET_STATUS:
            return "failed" if "incomplete" in type_name.lower() else "done"
        if status == DELETED_TICKET_STATUS:
            return "failed"
        return None

    def _queue_ticket_types(self, queue):
        queue = str(queue)
        with self._ticket_types_lock:
            if queue not in self._ticket_types:
                payload = self.client.get("support.ticket_type_list", params={"queue": queue})
                self._ticket_types[queue] = {
                    str(item.get("ticket_type_id")): {
                        "name": str(item.get("name") or ""),
                        "status": str(item.get("status") or ""),
                    }
                    for item in _as_items(payload.get("data"))
                }
            return self._ticket_types[queue]

    def _client_has_contract(self, client_id, recurring):
        """Si el cliente tuvo contrato alguna vez, se mantiene con contrato."""
        if self.store.has_contract(client_id):
            return True
        if self._any_contract(client_id, recurring):
            return True
        # Los servicios actuales no lo dicen: se busca en el historial (cancelados, cambios de plan).
        history, _ = self._list_services(client_id, pack_type_select=3)
        return self._any_contract(client_id, history)

    def _any_contract(self, client_id, services):
        for service in services:
            status = contract_term_status(service.get("contract_term"))
            if status is None:
                logger.warning(
                    "Cliente %s servicio %s: contract_term no reconocido %r; se trata como sin contrato",
                    client_id,
                    service["service_id"],
                    service.get("contract_term"),
                )
            elif status:
                self.store.mark_contract(client_id)
                return True
        return False

    @staticmethod
    def _client_balance(client_data):
        for key in ("balance", "inv_balance"):
            value = _parse_money(client_data.get(key))
            if value is not None:
                return value
        return None

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

    def _list_services(self, client_id, pack_type_select=4):
        """
        Devuelve (recurrentes, evaluables).

        recurrentes: servicios con period != 0 en cualquier estado (para el contrato).
        evaluables: recurrentes activos, pendientes o suspendidos que no han terminado.

        pack_type_select=4 trae los servicios vigentes; 3 trae todo el historial
        (mucho más lento en clientes antiguos), así que solo se usa para el contrato.
        """
        payload = self.client.get_paginated(
            "client.service_list",
            params={"client_id": client_id, "pack_type_select": pack_type_select, "metadata": 1},
            page_size=self.page_size,
        )
        now_ts = int(datetime.now(timezone.utc).timestamp())
        recurring = []
        evaluable = []
        for item in (payload.get("data") or {}).values():
            if not isinstance(item, dict):
                continue
            service_id = item.get("packid") or item.get("service_id")
            if not service_id:
                continue
            # period=0 son cargos únicos: reactivaciones, cambios de plan, equipos.
            if str(item.get("period") or "0") == "0":
                continue

            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            contract_term = metadata.get("contract_term", item.get("contract_term"))
            status_code = str(item.get("active") or "")
            service = {
                "service_id": str(service_id),
                "title": (item.get("title") or item.get("desserv") or item.get("code") or "").strip(),
                "code": str(item.get("code") or "").strip(),
                "status_code": status_code,
                "contract_term": str(contract_term) if contract_term is not None else None,
                "provider": str(metadata.get("provider") or "").strip(),
                "cid": metadata.get("cid"),
            }
            recurring.append(service)

            try:
                end_ts = int(item.get("end") or 0)
            except (TypeError, ValueError):
                end_ts = 0
            if (
                status_code in EVALUABLE_SERVICE_CODES
                and not (end_ts and end_ts < now_ts)
                and not is_fiberx_service(service)
                and not is_ignored_service(service, self.ignored_service_ids)
            ):
                evaluable.append(service)
        return recurring, evaluable

    def _list_overdue_invoices(self, client_id, today):
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
            if not due_day or due_day >= today:
                continue
            invid = str(item.get("invid") or "")
            if not invid:
                continue
            invoices.append(
                {
                    "invid": invid,
                    "amount_unpaid": str(_parse_money(item.get("amount_unpaid") or item.get("amount")) or 0),
                    "due_date": due_day,
                    "days_overdue": (today - due_day).days,
                    "packs": self._invoice_packs(invid),
                }
            )
        invoices.sort(key=lambda item: item["due_date"] or date.max)
        return invoices

    def _invoice_packs(self, invid):
        """
        Pares (packid, parentpack) facturados. Si Ubersmith falla se propaga el error:
        decidir sin saber a qué servicio pertenece la deuda podría cortar o reconectar mal.
        """
        cached = self.store.get_invoice_packs(invid)
        if cached is not None:
            return [tuple(pair) for pair in cached]
        detail = self.client.get("client.invoice_get", params={"invoice_id": invid}).get("data") or {}
        packs = []
        for section in ("current_packs", "pre_billed_packs"):
            for line in _as_items(detail.get(section)):
                if not invoice_line_is_unpaid(line):
                    continue
                packid = str(line.get("packid") or "")
                if packid:
                    packs.append([packid, str(line.get("parentpack") or "0")])
        self.store.cache_invoice_packs(invid, packs)
        return [tuple(pair) for pair in packs]

    def _find_blocking_open_ticket(self, client_id, queue, action, service_id):
        """
        Si ya hay un ticket abierto en la cola, del mismo cliente y servicio,
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
                "type": ticket.get("type_name") or ticket.get("type") or "",
            }
        return None

    def _list_open_queue_tickets(self, client_id, queue, since):
        open_types = [
            type_id
            for type_id, info in self._queue_ticket_types(queue).items()
            if info["status"] in OPEN_TICKET_STATUSES
        ]
        if not open_types:
            raise UbersmithAPIError(f"La cola {queue} no devolvió tipos de ticket abiertos")
        tickets = []
        seen = set()
        since_ts = int(since.timestamp())
        for ticket_type in open_types:
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

    def _ticket_body(self, client_id, action, decisions, balance, services, comments, invoices, has_contract):
        titles = {item["service_id"]: item["title"] for item in services}
        target_lines = [
            f"- Servicio {item['service_id']}: {titles.get(item['service_id'], '')} — {item['reason']}"
            for item in decisions
        ]
        targets = {item["service_id"] for item in decisions}
        other_lines = [
            f"- Servicio {item['service_id']}: {item['title']}"
            for item in services
            if item["service_id"] not in targets
        ]
        invoice_lines = [
            f"- Factura {item['invid']}: vence {item['due_date']}, "
            f"impago {item['amount_unpaid']}, {item['days_overdue']} días de atraso"
            for item in invoices
        ]
        comment_lines = [
            f"- {comment.get('time') or 'sin fecha'}: {comment.get('text')}" for comment in comments
        ]
        return (
            f"Solicitud automática de {ACTION_LABELS[action].lower()}.\n\n"
            f"Cliente: {client_id}\n"
            f"Acción: {action}\n"
            f"Balance: {balance if balance is not None else 'N/D'}\n"
            f"Contrato: {'sí' if has_contract else 'no'}\n\n"
            f"Servicios a {'desconectar' if action == 'disconnect' else 'reconectar'}:\n"
            f"{chr(10).join(target_lines)}\n\n"
            f"Facturas vencidas:\n"
            f"{chr(10).join(invoice_lines) or '- Ninguna'}\n\n"
            f"Otros servicios del cliente:\n"
            f"{chr(10).join(other_lines) or '- Ninguno'}\n\n"
            f"Comentarios recientes:\n"
            f"{chr(10).join(comment_lines) or '- Sin comentarios'}\n"
        )
