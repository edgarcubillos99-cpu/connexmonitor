import calendar
import logging
import os
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from core.ubersmith_client import UbersmithClient

logger = logging.getLogger(__name__)

BUSINESS_TZ = ZoneInfo("America/Puerto_Rico")
CONTRACT_GRACE_MONTHS = 2
# Un "Desconectado" de 2015 no debe mandar a reconectar si el cliente ya opera normal.
CONNECTION_STATUS_MAX_AGE_DAYS = 548

# Comentarios reales vistos en Ubersmith: "Contrato", "Contrato 2 años",
# "fuera de contrato", "Desconectado por Balance Pendiente", "Reconectado".
CONTRACT_POSITIVE_RE = re.compile(
    r"(?i)("
    r"^\s*contrato\.?\s*$"
    r"|bajo contrato"
    r"|con contrato"
    r"|tiene contrato"
    r"|cuenta con contrato"
    r"|renov[oó]\s+contrato"
    r"|renovaci[oó]n de contrato"
    r"|contrato\s*\d+\s*a[nñ]os?"
    r"|\d+\s*a[nñ]os?\s+contrato"
    r"|\*contrato\b"
    r")"
)
CONTRACT_NEGATIVE_RE = re.compile(
    r"(?i)("
    r"sin contrato"
    r"|fuera de contrato"
    r"|no contrato"
    r"|no (?:estaba |est[aá] )?bajo contrato"
    r"|no (?:tiene|cuenta con) contrato"
    r")"
)
DISCONNECT_STATUS_RE = re.compile(
    r"(?i)^(?:cliente\s+)?desconectad[oa]s?\b"
    r"|se desconect[oó] el servicio"
    r"|se encuentra desconectado"
)
RECONNECT_STATUS_RE = re.compile(
    r"(?i)^(?:cliente\s+)?reconectad[oa]s?\b"
)


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


def add_months(day, months):
    """Suma meses de calendario sin pasarse del último día del mes destino."""
    month_index = day.month - 1 + months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day.day, last_day))


def today_business():
    return datetime.now(BUSINESS_TZ).date()


class ConnexBot:
    def __init__(self, client=None):
        self.client = client or UbersmithClient()
        self.support_queue_id = 57
        self.dry_run = os.getenv("CONNEX_DRY_RUN", "1") != "0"
        self.page_size = int(os.getenv("CONNEX_PAGE_SIZE", self.client.page_size))

    def process_clients(self, max_clients=None):
        """
        Flujo principal del bot (solo GET):
        1. Obtener clientes por páginas.
        2. Revisar comentarios: contrato y desconexión.
        3. Revisar facturas impagas y mora.
        4. Preparar tickets de desconexión o reconexión (sin POST).
        """
        summary = {
            "clients_scanned": 0,
            "with_contract": 0,
            "disconnected": 0,
            "overdue_beyond_grace": 0,
            "reconnect_candidates": 0,
            "actions": [],
        }

        clients = self._list_active_clients()
        logger.info("Clientes activos obtenidos: %s", len(clients))

        for client_id, client_data in clients.items():
            if max_clients is not None and summary["clients_scanned"] >= max_clients:
                break
            summary["clients_scanned"] += 1
            comments = self._list_comments(client_id)
            profile = self._analyze_comments(comments)
            balance = _parse_money(
                client_data.get("balance")
                if client_data.get("balance") not in (None, "")
                else client_data.get("inv_balance")
            )

            if profile["has_contract"]:
                summary["with_contract"] += 1
            if profile["is_disconnected"]:
                summary["disconnected"] += 1
                action = self.handle_disconnected_client(
                    client_id,
                    balance=balance,
                    disconnect_comment=profile["disconnect_comment"],
                )
            else:
                invoices = self.check_unpaid_invoices(client_id)
                action = self.handle_overdue_client(
                    client_id,
                    profile["has_contract"],
                    invoices.get("oldest_due"),
                    invoices=invoices,
                )

            if action:
                summary["actions"].append(action)
                if action.get("type") == "disconnect":
                    summary["overdue_beyond_grace"] += 1
                elif action.get("type") == "reconnect":
                    summary["reconnect_candidates"] += 1

        logger.info(
            "Escaneo terminado: %s clientes, %s acciones (dry_run=%s)",
            summary["clients_scanned"],
            len(summary["actions"]),
            self.dry_run,
        )
        return summary

    def check_client_contract(self, client_id):
        """
        Revisa los comentarios de un cliente para extraer si tiene contrato.
        (Usa client.comment_list)
        """
        comments = self._list_comments(client_id)
        return self._analyze_comments(comments)["has_contract"]

    def check_unpaid_invoices(self, client_id):
        """
        Verifica si el cliente tiene un retraso con el pago de sus servicios.
        (Usa client.invoice_list con paid=0)
        """
        payload = self.client.get_paginated(
            "client.invoice_list",
            params={
                "client_id": client_id,
                "paid": 0,
                "order_by": "due",
                "direction": "asc",
            },
            page_size=self.page_size,
        )
        invoices = []
        oldest_due = None
        unpaid_total = Decimal("0")

        for invoice in (payload.get("data") or {}).values():
            if not isinstance(invoice, dict):
                continue
            if str(invoice.get("paid", "0")) not in {"0", "false"}:
                continue
            due_dt = _parse_unix_datetime(invoice.get("due"))
            due_day = due_dt.date() if due_dt else None
            amount_unpaid = _parse_money(invoice.get("amount_unpaid") or invoice.get("amount"))
            unpaid_total += amount_unpaid
            invoices.append(
                {
                    "invid": invoice.get("invid"),
                    "amount": invoice.get("amount"),
                    "amount_unpaid": str(amount_unpaid),
                    "due": invoice.get("due"),
                    "due_date": due_day.isoformat() if due_day else None,
                }
            )
            if due_day and (oldest_due is None or due_day < oldest_due):
                oldest_due = due_day

        today = today_business()
        overdue_days = (today - oldest_due).days if oldest_due and today > oldest_due else 0
        return {
            "has_unpaid": bool(invoices),
            "invoices": invoices,
            "unpaid_total": unpaid_total,
            "oldest_due": oldest_due,
            "overdue": bool(oldest_due and today > oldest_due),
            "overdue_days": overdue_days,
        }

    def handle_overdue_client(self, client_id, has_contract, overdue_time, invoices=None):
        """
        Si tiene contrato, puede demorarse 2 meses. Si no, no hay prórroga.
        Prepara un ticket en la queue 57 si excede el tiempo (sin POST).
        """
        invoices = invoices or {}
        oldest_due = overdue_time or invoices.get("oldest_due")
        if not oldest_due:
            return None

        today = today_business()
        if today <= oldest_due:
            return None

        grace_deadline = add_months(oldest_due, CONTRACT_GRACE_MONTHS) if has_contract else oldest_due
        if today <= grace_deadline:
            logger.debug(
                "Cliente %s en mora pero dentro de prórroga (contrato=%s, vence=%s, límite=%s)",
                client_id,
                has_contract,
                oldest_due,
                grace_deadline,
            )
            return None

        invoice_lines = invoices.get("invoices") or []
        invoice_text = "\n".join(
            f"- Factura {item.get('invid')} vencida {item.get('due_date')} "
            f"(impago {item.get('amount_unpaid')})"
            for item in invoice_lines
        ) or "- Sin detalle de facturas"
        grace_label = f"{CONTRACT_GRACE_MONTHS} meses" if has_contract else "sin prórroga"

        subject = f"Desconexión de servicios - Cliente {client_id}"
        body = (
            f"Solicitud automática de desconexión.\n\n"
            f"Cliente: {client_id}\n"
            f"Contrato: {'sí' if has_contract else 'no'}\n"
            f"Prórroga aplicable: {grace_label}\n"
            f"Factura impaga más antigua (due): {oldest_due.isoformat()}\n"
            f"Límite de mora: {grace_deadline.isoformat()}\n"
            f"Días de atraso: {invoices.get('overdue_days', (today - oldest_due).days)}\n"
            f"Total impago: {invoices.get('unpaid_total', 'N/D')}\n\n"
            f"Facturas:\n{invoice_text}\n"
        )
        ticket = self.create_support_ticket(client_id, subject, body)
        return {"type": "disconnect", "client_id": str(client_id), "ticket": ticket}

    def handle_disconnected_client(self, client_id, balance=None, disconnect_comment=None):
        """
        Si el cliente tiene el comentario de desconectado y su pago ya se registró,
        se prepara un ticket en la queue 57 de reconexión (sin POST).
        """
        if balance is None:
            detail = self.client.get("client.get", params={"client_id": client_id})
            client_data = detail.get("data") or {}
            balance = _parse_money(
                client_data.get("balance")
                if client_data.get("balance") not in (None, "")
                else client_data.get("inv_balance")
            )

        invoices = self.check_unpaid_invoices(client_id)
        paid_up = (not invoices["has_unpaid"]) or balance == 0
        if not paid_up:
            logger.debug(
                "Cliente %s sigue desconectado con saldo %s y %s facturas impagas",
                client_id,
                balance,
                len(invoices["invoices"]),
            )
            return None

        subject = f"Reconexión de servicios - Cliente {client_id}"
        body = (
            f"Solicitud automática de reconexión.\n\n"
            f"Cliente: {client_id}\n"
            f"Estado en comentarios: desconectado\n"
            f"Comentario: {disconnect_comment or 'N/D'}\n"
            f"Balance: {balance}\n"
            f"Facturas impagas: {len(invoices['invoices'])}\n"
            f"El pago ya figura registrado (facturas pagadas o balance en 0).\n"
        )
        ticket = self.create_support_ticket(client_id, subject, body)
        return {"type": "reconnect", "client_id": str(client_id), "ticket": ticket}

    def create_support_ticket(self, client_id, subject, body):
        """
        Prepara un ticket en Ubersmith para un cliente.
        POST (support.ticket_submit) queda deshabilitado a propósito.
        """
        ticket = {
            "dry_run": True,
            "queue": self.support_queue_id,
            "client_id": str(client_id),
            "subject": subject,
            "body": body,
        }
        logger.info(
            "Ticket en cola (sin POST) queue=%s client=%s subject=%s",
            self.support_queue_id,
            client_id,
            subject,
        )
        return ticket

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
        return comments

    def _analyze_comments(self, comments):
        has_contract = False
        contract_comment = None
        is_disconnected = False
        disconnect_comment = None
        connection_resolved = False

        for comment in comments:
            text = (comment.get("comment") or "").strip()
            if not text:
                continue
            first_line = text.splitlines()[0].strip()

            comment_dt = _parse_unix_datetime(comment.get("time"))
            comment_age_days = (
                (today_business() - comment_dt.date()).days if comment_dt else None
            )
            status_is_fresh = (
                comment_age_days is None or comment_age_days <= CONNECTION_STATUS_MAX_AGE_DAYS
            )

            if not connection_resolved and status_is_fresh:
                if RECONNECT_STATUS_RE.search(first_line) or RECONNECT_STATUS_RE.search(text):
                    is_disconnected = False
                    disconnect_comment = text
                    connection_resolved = True
                elif DISCONNECT_STATUS_RE.search(first_line) or DISCONNECT_STATUS_RE.search(text):
                    is_disconnected = True
                    disconnect_comment = text
                    connection_resolved = True

            if contract_comment is None:
                verdict = self._classify_contract_comment(text)
                if verdict is not None:
                    has_contract = verdict
                    contract_comment = text

            if connection_resolved and contract_comment is not None:
                break

        return {
            "has_contract": has_contract,
            "contract_comment": contract_comment,
            "is_disconnected": is_disconnected,
            "disconnect_comment": disconnect_comment,
        }

    @staticmethod
    def _classify_contract_comment(text):
        if CONTRACT_NEGATIVE_RE.search(text):
            return False
        if CONTRACT_POSITIVE_RE.search(text):
            return True
        return None
