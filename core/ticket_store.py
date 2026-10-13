import json
import logging
import os
import uuid
from datetime import datetime, timezone

import redis

logger = logging.getLogger(__name__)

HISTORY_KEY = "connex:history:{client_id}"
ANALYSIS_KEY = "connex:analysis:{fingerprint}"
CIRCUIT_KEY = "connex:circuit:{service_id}"
CLIENT_CIRCUITS_KEY = "connex:client_circuits:{client_id}"
CLIENTS_WITH_CIRCUITS_KEY = "connex:clients_with_circuits"
CONTRACT_CLIENTS_KEY = "connex:contract_clients"
INVOICE_PACKS_KEY = "connex:invoice_packs:v2:{invid}"
FAILED_KEY = "connex:failed:{service_id}:{action}"
LOCK_KEY = "connex:cycle_lock"
HISTORY_LIMIT = 20
INVOICE_PACKS_TTL = 60 * 60 * 24 * 90

STATE_ACTIVE = "active"
STATE_DISCONNECT_REQUESTED = "disconnect_requested"
STATE_DISCONNECTED = "disconnected"
STATE_RECONNECT_REQUESTED = "reconnect_requested"

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


def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


class TicketStore:
    """
    Redis como base propia del bot.

    Un circuito es un servicio recurrente de Ubersmith (service_id). Solo se
    guardan los circuitos que no están "active"; sin registro = activo.
    """

    def __init__(self, url=None):
        self.client = redis.from_url(
            url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        self.client.ping()

    def get_client_circuits(self, client_id):
        service_ids = self.client.smembers(CLIENT_CIRCUITS_KEY.format(client_id=client_id))
        circuits = {}
        for service_id in service_ids:
            record = self.client.hgetall(CIRCUIT_KEY.format(service_id=service_id))
            if record:
                circuits[service_id] = record
        return circuits

    def client_has_circuits(self, client_id):
        return bool(self.client.sismember(CLIENTS_WITH_CIRCUITS_KEY, str(client_id)))

    def set_circuit(self, client_id, service_id, state, **fields):
        client_id = str(client_id)
        service_id = str(service_id)
        if state == STATE_ACTIVE:
            self.delete_circuit(client_id, service_id)
            return
        record = {
            "client_id": client_id,
            "service_id": service_id,
            "state": state,
            "updated_at": _utcnow_iso(),
        }
        record.update({key: "" if value is None else str(value) for key, value in fields.items()})
        pipe = self.client.pipeline()
        pipe.delete(CIRCUIT_KEY.format(service_id=service_id))
        pipe.hset(CIRCUIT_KEY.format(service_id=service_id), mapping=record)
        pipe.sadd(CLIENT_CIRCUITS_KEY.format(client_id=client_id), service_id)
        pipe.sadd(CLIENTS_WITH_CIRCUITS_KEY, client_id)
        pipe.execute()

    def delete_circuit(self, client_id, service_id):
        client_id = str(client_id)
        client_key = CLIENT_CIRCUITS_KEY.format(client_id=client_id)
        pipe = self.client.pipeline()
        pipe.delete(CIRCUIT_KEY.format(service_id=service_id))
        pipe.srem(client_key, str(service_id))
        pipe.execute()
        if not self.client.scard(client_key):
            self.client.srem(CLIENTS_WITH_CIRCUITS_KEY, client_id)

    def mark_failed(self, service_id, action, ticket_id, ttl_seconds):
        self.client.setex(
            FAILED_KEY.format(service_id=service_id, action=action),
            max(60, int(ttl_seconds)),
            str(ticket_id or ""),
        )

    def recently_failed(self, service_id, action):
        return self.client.get(FAILED_KEY.format(service_id=service_id, action=action))

    def has_contract(self, client_id):
        return bool(self.client.sismember(CONTRACT_CLIENTS_KEY, str(client_id)))

    def mark_contract(self, client_id):
        self.client.sadd(CONTRACT_CLIENTS_KEY, str(client_id))

    def get_invoice_packs(self, invid):
        raw = self.client.get(INVOICE_PACKS_KEY.format(invid=invid))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def cache_invoice_packs(self, invid, packs):
        self.client.setex(INVOICE_PACKS_KEY.format(invid=invid), INVOICE_PACKS_TTL, json.dumps(packs))

    def record_request(self, record):
        history_key = HISTORY_KEY.format(client_id=record["client_id"])
        pipe = self.client.pipeline()
        pipe.lpush(history_key, json.dumps(record, ensure_ascii=False, default=str))
        pipe.ltrim(history_key, 0, HISTORY_LIMIT - 1)
        pipe.execute()

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
