import json
import logging
import os
import uuid
from datetime import datetime, timezone

import redis

logger = logging.getLogger(__name__)

LAST_KEY = "connex:last:{client_id}"
HISTORY_KEY = "connex:history:{client_id}"
ANALYSIS_KEY = "connex:analysis:{fingerprint}"
LOCK_KEY = "connex:cycle_lock"
HISTORY_LIMIT = 20

# Solo el dueño del candado (mismo token) puede renovarlo o liberarlo.
REFRESH_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""
RELEASE_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


def _utcnow():
    return datetime.now(timezone.utc)


class TicketStore:
    """Redis: última solicitud por cliente, historial corto y caché de análisis."""

    def __init__(self, url=None):
        self.client = redis.from_url(
            url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        self.client.ping()

    def get_last_request(self, client_id):
        raw = self.client.get(LAST_KEY.format(client_id=client_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Registro Redis corrupto para cliente %s", client_id)
            return None

    def should_skip(self, client_id, action, situation_hash, service_id, cooldown_hours):
        """
        Evita repetir la misma solicitud reciente.

        Se considera reincidencia si, dentro del cooldown, ya existe un ticket
        con la misma acción, el mismo servicio y la misma situación.
        Una situación nueva (pago, cambio de servicio o acción opuesta) sí pasa.
        """
        last = self.get_last_request(client_id)
        if not last:
            return False, "sin_registro"

        last_action = last.get("action")
        last_service = str(last.get("service_id") or "")
        wanted_service = str(service_id or "")
        if last_action != action:
            return False, "accion_distinta"
        if last_service != wanted_service:
            return False, "servicio_distinto"
        if last.get("situation_hash") != situation_hash:
            return False, "situacion_nueva"

        created_at = _parse_iso(last.get("created_at"))
        if created_at is None:
            return False, "fecha_invalida"

        age_hours = (_utcnow() - created_at).total_seconds() / 3600
        if age_hours < cooldown_hours:
            return True, (
                f"ticket_reciente ticket_id={last.get('ticket_id')} "
                f"hace_{int(age_hours)}h"
            )
        return False, "cooldown_vencido"

    def record_request(self, record):
        client_id = record["client_id"]
        payload = json.dumps(record, ensure_ascii=False)
        self.client.set(LAST_KEY.format(client_id=client_id), payload)
        history_key = HISTORY_KEY.format(client_id=client_id)
        self.client.lpush(history_key, payload)
        self.client.ltrim(history_key, 0, HISTORY_LIMIT - 1)

    def get_cached_analysis(self, fingerprint):
        raw = self.client.get(ANALYSIS_KEY.format(fingerprint=fingerprint))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def cache_analysis(self, fingerprint, analysis, ttl_seconds):
        self.client.setex(
            ANALYSIS_KEY.format(fingerprint=fingerprint),
            ttl_seconds,
            json.dumps(analysis, ensure_ascii=False),
        )

    def acquire_lock(self, ttl_seconds):
        token = uuid.uuid4().hex
        if self.client.set(LOCK_KEY, token, nx=True, ex=max(60, int(ttl_seconds))):
            return token
        return None

    def refresh_lock(self, token, ttl_seconds):
        return bool(
            self.client.eval(REFRESH_LOCK_SCRIPT, 1, LOCK_KEY, token, max(60, int(ttl_seconds)))
        )

    def release_lock(self, token):
        self.client.eval(RELEASE_LOCK_SCRIPT, 1, LOCK_KEY, token)


def _parse_iso(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
