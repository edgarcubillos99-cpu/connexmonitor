import json
import logging
import os

from openai import OpenAI

logger = logging.getLogger(__name__)

VALID_ACTIONS = {"disconnect", "reconnect", "none"}

SYSTEM_PROMPT = """Eres un analista de operaciones de un ISP.
Revisas comentarios internos de Ubersmith y un resumen del cliente.
Debes decidir UNA acción para solicitar al departamento correspondiente:

- disconnect: hay que pedir desconexión del servicio.
- reconnect: hay que pedir reconexión del servicio.
- none: no hay que solicitar nada ahora.

Reglas:
- Basa la decisión principalmente en los comentarios, usando balance y servicios como contexto.
- No inventes hechos que no estén en los datos.
- Un comentario viejo de desconexión no justifica reconectar si el cliente ya opera normal o hay uno más reciente de reconexión.
- Si el cliente figura desconectado y el balance ya está en cero / sin deuda, reconectar tiene sentido.
- Si hay mora o instrucción de cortar, y no hay prórroga clara vigente, desconectar tiene sentido.
- Si la evidencia es ambigua o insuficiente, usa none.
- Si mencionan un servicio concreto y está en la lista, inclúyelo en service_ids.

Responde SOLO un JSON con esta forma:
{
  "action": "disconnect" | "reconnect" | "none",
  "reason": "motivo breve en español",
  "service_ids": ["id", "..."],
  "confidence": 0.0
}
"""


class CommentAnalyzer:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("Falta OPENAI_API_KEY en las variables de entorno.")
        self.client = OpenAI(api_key=api_key)
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.min_confidence = float(os.getenv("CONNEX_MIN_CONFIDENCE", "0.7"))

    def analyze(self, client_id, comments, services, balance, balance_bucket):
        user_payload = {
            "client_id": str(client_id),
            "balance": str(balance),
            "balance_state": balance_bucket,
            "services": services,
            "comments": comments,
        }
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": json.dumps(user_payload, ensure_ascii=False),
                    },
                ],
            )
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
            "reason": str(parsed.get("reason") or "").strip() or "Sin motivo",
            "service_ids": service_ids,
            "confidence": confidence,
        }
