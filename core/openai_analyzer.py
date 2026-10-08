import json
import logging
import os
import re

from openai import OpenAI

logger = logging.getLogger(__name__)

VALID_ACTIONS = {"disconnect", "reconnect", "none"}

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
MASKED_CARD_RE = re.compile(r"\*{2,}\s*\d{4}")
LONG_NUMBER_RE = re.compile(r"(?<!\d)\d{9,19}(?!\d)")
PHONE_RE = re.compile(r"(?<!\d)(?:\(\d{3}\)\s*|\d{3}[-.\s])\d{3}[-.\s]\d{4}(?!\d)")

SYSTEM_PROMPT = """Eres un analista de operaciones de un ISP.
Revisas comentarios internos de Ubersmith y un resumen del cliente.
Debes decidir UNA acción para solicitar al departamento correspondiente:

- disconnect: hay que pedir desconexión del servicio.
- reconnect: hay que pedir reconexión del servicio.
- none: no hay que solicitar nada ahora.

Datos que recibes:
- today: fecha de hoy (America/Puerto_Rico). Úsala para saber si una promesa o prórroga ya venció.
- unpaid_invoices: facturas impagas con su fecha de vencimiento (due_date) y días de atraso.
- balance / balance_state, services (status: active, pending, suspended) y comments (más recientes primero).

Reglas de negocio:
- Contrato: determina por los comentarios si el cliente tiene contrato vigente
  (ej. "Contrato", "Contrato 2 años", "renovó contrato"). "Sin contrato", "no contrato"
  o "fuera de contrato" significa que NO tiene. Si no hay evidencia, has_contract = false.
- Sin contrato no hay prórroga: si el due_date ya pasó, hay que pedir disconnect.
- Con contrato, el cliente puede durar máximo 61 días de atraso desde el due_date
  de su factura impaga más antigua. Al día 62, disconnect.
- Nunca pidas disconnect si ninguna factura impaga está vencida.
- Una promesa de pago documentada que aún no vence es una prórroga vigente; si ya venció, no lo es.

Reglas generales:
- Basa la decisión en los comentarios, usando facturas, balance y servicios como contexto.
- No inventes hechos que no estén en los datos.
- Un comentario viejo de desconexión no justifica reconectar si el cliente ya opera normal o hay uno más reciente de reconexión.
- Si el cliente figura desconectado y el balance ya está en cero / sin deuda, reconectar tiene sentido.
- Si hay mora o instrucción de cortar, y no hay prórroga clara vigente, desconectar tiene sentido.
- Si la evidencia es ambigua o insuficiente, usa none.
- Si mencionan un servicio concreto y está en la lista, inclúyelo en service_ids.

Responde SOLO un JSON con esta forma:
{
  "action": "disconnect" | "reconnect" | "none",
  "has_contract": true | false,
  "reason": "motivo breve en español",
  "service_ids": ["id", "..."],
  "confidence": 0.0
}
"""


def redact_personal_data(text):
    text = EMAIL_RE.sub("[email]", text)
    text = MASKED_CARD_RE.sub("[tarjeta]", text)
    text = LONG_NUMBER_RE.sub("[numero]", text)
    return PHONE_RE.sub("[telefono]", text)


class CommentAnalyzer:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("Falta OPENAI_API_KEY en las variables de entorno.")
        self.client = OpenAI(api_key=api_key)
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.min_confidence = float(os.getenv("CONNEX_MIN_CONFIDENCE", "0.7"))

    def analyze(self, client_id, comments, services, invoices, balance, balance_bucket, today):
        user_payload = {
            "client_id": str(client_id),
            "today": today.isoformat(),
            "balance": str(balance),
            "balance_state": balance_bucket,
            "unpaid_invoices": [
                {
                    "invid": item["invid"],
                    "amount_unpaid": item["amount_unpaid"],
                    "due_date": item["due_date"].isoformat() if item["due_date"] else None,
                    "days_overdue": item["days_overdue"],
                }
                for item in invoices
            ],
            "services": services,
            "comments": [
                {**comment, "text": redact_personal_data(comment.get("text") or "")}
                for comment in comments
            ],
        }
        try:
            create_kwargs = {
                "model": self.model,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": json.dumps(user_payload, ensure_ascii=False),
                    },
                ],
            }
            # gpt-5.6-luna (y otros gpt-5) no aceptan temperature=0.
            if not str(self.model).startswith("gpt-5"):
                create_kwargs["temperature"] = 0
            response = self.client.chat.completions.create(**create_kwargs)
        except Exception as exc:
            logger.exception("Fallo al consultar OpenAI para cliente %s", client_id)
            raise RuntimeError(f"OpenAI no respondió para cliente {client_id}: {exc}") from exc

        raw = (response.choices[0].message.content or "").strip()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"OpenAI devolvió JSON inválido para cliente {client_id}") from exc

        action = str(parsed.get("action") or "none").strip().lower()
        if action not in VALID_ACTIONS:
            action = "none"

        try:
            confidence = float(parsed.get("confidence", 0))
        except (TypeError, ValueError):
            confidence = 0.0

        service_ids = []
        raw_services = parsed.get("service_ids") or []
        if isinstance(raw_services, (str, int)):
            raw_services = [raw_services]
        known = {str(item.get("service_id")) for item in services if item.get("service_id")}
        for item in raw_services:
            service_id = str(item).strip()
            if service_id and service_id in known:
                service_ids.append(service_id)

        if action != "none" and confidence < self.min_confidence:
            logger.info(
                "Cliente %s: OpenAI dijo %s con confianza %.2f < %.2f; se ignora",
                client_id,
                action,
                confidence,
                self.min_confidence,
            )
            action = "none"

        return {
            "action": action,
            "has_contract": parsed.get("has_contract") is True,
            "reason": str(parsed.get("reason") or "").strip() or "Sin motivo",
            "service_ids": service_ids,
            "confidence": confidence,
        }
