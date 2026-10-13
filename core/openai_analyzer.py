import json
import logging
import os
import re

from openai import OpenAI

logger = logging.getLogger(__name__)

VALID_ACTIONS = {"disconnect", "none"}

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
MASKED_CARD_RE = re.compile(r"\*{2,}\s*\d{4}")
LONG_NUMBER_RE = re.compile(r"(?<!\d)\d{9,19}(?!\d)")
PHONE_RE = re.compile(r"(?<!\d)(?:\(\d{3}\)\s*|\d{3}[-.\s])\d{3}[-.\s]\d{4}(?!\d)")

SYSTEM_PROMPT = """Eres un auditor de operaciones de un ISP.
El sistema automático ha decidido DESCONECTAR al cliente por falta de pago.
Tu único trabajo es revisar el historial de comentarios del cliente y buscar si un agente humano le ha otorgado una prórroga, promesa de pago o excepción que aún esté vigente para la fecha de hoy.

Datos que recibes:
- today: fecha de hoy (America/Puerto_Rico).
- comments: comentarios del cliente, del más reciente al más antiguo.

Reglas:
1. Si encuentras una prórroga o promesa de pago que vence EN EL FUTURO (después de 'today') o HOY, debes detener la desconexión.
2. Si la prórroga ya venció en el pasado (antes de 'today'), la desconexión procede.
3. Si no hay comentarios sobre prórrogas o excepciones, la desconexión procede.
4. No asumas prórrogas si no hay evidencia clara en el texto.

Responde SOLO un JSON con esta forma:
{
  "action": "none" | "disconnect",
  "reason": "motivo por el cual detienes (none) o permites (disconnect) la desconexión"
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

    def check_extensions(self, client_id, comments, today):
        user_payload = {
            "client_id": str(client_id),
            "today": today.isoformat(),
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

        # Ante cualquier respuesta que no sea un "disconnect" explícito, no se corta.
        action = str(parsed.get("action") or "none").strip().lower()
        if action not in VALID_ACTIONS:
            logger.warning("Cliente %s: OpenAI respondió acción inválida %r; no se corta", client_id, action)
            action = "none"

        return {
            "action": action,
            "reason": str(parsed.get("reason") or "").strip() or "Sin motivo",
        }
