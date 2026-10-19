import json
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone

import pymysql
from pymysql.constants import CLIENT
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)

STATE_ACTIVE = "active"
STATE_DISCONNECT_REQUESTED = "disconnect_requested"
STATE_DISCONNECTED = "disconnected"
STATE_RECONNECT_REQUESTED = "reconnect_requested"
STATUS_CONNECTED = "conectado"
STATUS_DISCONNECTED = "desconectado"
# Lo que el inventario muestra como desconectado (visión de los inspectores).
DISCONNECTED_STATES = {
    STATE_DISCONNECTED,
    STATE_DISCONNECT_REQUESTED,
    STATE_RECONNECT_REQUESTED,
}

# Origen del registro: un ticket de este bot o la línea base del primer ciclo.
# reconnect_requested lo escribe el servicio de reconexiones, no este bot.
SOURCE_TICKET = "ticket"
SOURCE_BASELINE = "baseline"

INVOICE_PACKS_TTL = 60 * 60 * 24 * 90
# Candado de este bot (desconexiones). El de reconexiones debe usar otro name.
LOCK_NAME = "cycle"
BASELINE_META_KEY = "baseline_started_at"
CIRCUIT_FIELDS = ("cid", "ticket_id", "queue", "previous_state", "reason", "requested_at", "source")
DATABASE_NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,64}$")
UNKNOWN_DATABASE = 1049
DUPLICATE_KEY_NAME = 1061
# Conexión caída o cerrada por el servidor remoto: se reabre y se reintenta una vez.
RECONNECT_ERRORS = {0, 2006, 2013, 2014, 2055}

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS connex_circuits (
        service_id VARCHAR(32) NOT NULL PRIMARY KEY,
        client_id VARCHAR(32) NOT NULL,
        state VARCHAR(32) NOT NULL,
        cid VARCHAR(64) NULL,
        ticket_id VARCHAR(32) NULL,
        queue VARCHAR(16) NULL,
        previous_state VARCHAR(32) NULL,
        reason TEXT NULL,
        requested_at VARCHAR(40) NULL,
        source VARCHAR(16) NOT NULL DEFAULT 'ticket',
        updated_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_contract_clients (
        client_id VARCHAR(32) NOT NULL PRIMARY KEY,
        marked_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_invoice_packs (
        invid VARCHAR(32) NOT NULL PRIMARY KEY,
        packs TEXT NOT NULL,
        expires_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_failed (
        service_id VARCHAR(32) NOT NULL,
        action VARCHAR(16) NOT NULL,
        ticket_id VARCHAR(32) NOT NULL DEFAULT '',
        expires_at DATETIME NOT NULL,
        PRIMARY KEY (service_id, action)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_analysis_cache (
        fingerprint CHAR(64) NOT NULL PRIMARY KEY,
        analysis TEXT NOT NULL,
        expires_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_requests (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        client_id VARCHAR(32) NOT NULL,
        action VARCHAR(16) NOT NULL,
        ticket_id VARCHAR(32) NULL,
        queue VARCHAR(16) NULL,
        service_ids TEXT NOT NULL,
        created_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_meta (
        name VARCHAR(64) NOT NULL PRIMARY KEY,
        value TEXT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_baseline_pending (
        client_id VARCHAR(32) NOT NULL PRIMARY KEY
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_lock (
        name VARCHAR(32) NOT NULL PRIMARY KEY,
        token VARCHAR(64) NOT NULL DEFAULT '',
        expires_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS connex_services (
        service_id VARCHAR(32) NOT NULL PRIMARY KEY,
        client_id VARCHAR(32) NOT NULL,
        estado VARCHAR(16) NOT NULL,
        updated_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)

# Índices secundarios (las claves primarias van en SCHEMA). Se agregan también a
# tablas que ya existían, por eso no van dentro del CREATE TABLE.
INDEXES = (
    ("connex_circuits", "idx_circuits_client", "client_id"),
    ("connex_circuits", "idx_circuits_state_source", "state, source"),
    ("connex_circuits", "idx_circuits_ticket", "ticket_id"),
    ("connex_services", "idx_services_client", "client_id"),
    ("connex_services", "idx_services_estado", "estado"),
    ("connex_invoice_packs", "idx_invoice_packs_expires", "expires_at"),
    ("connex_analysis_cache", "idx_analysis_expires", "expires_at"),
    ("connex_failed", "idx_failed_expires", "expires_at"),
    ("connex_requests", "idx_requests_client", "client_id, created_at"),
    ("connex_requests", "idx_requests_ticket", "ticket_id"),
    ("connex_requests", "idx_requests_action_created", "action, created_at"),
)


def _utc_naive(value=None):
    """DATETIME de MySQL sin zona: todo se guarda en UTC."""
    moment = value or datetime.now(timezone.utc)
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
    return moment


class TicketStore:
    """
    MySQL como base propia del bot.

    Un circuito es un servicio recurrente de Ubersmith (service_id). Solo se
    guardan los circuitos que no están "active"; sin registro = activo.
    La base se crea si no existe (MYSQL_DATABASE); las tablas también.
    """

    def __init__(self):
        self.database = os.getenv("MYSQL_DATABASE", "connexmonitor").strip()
        if not DATABASE_NAME_RE.match(self.database):
            raise ValueError("MYSQL_DATABASE solo admite letras, números y _ (máx. 64)")
        host = (os.getenv("MYSQL_HOST") or "").strip()
        user = (os.getenv("MYSQL_USER") or "").strip()
        if not host or not user:
            raise ValueError("Faltan MYSQL_HOST y MYSQL_USER en las variables de entorno.")
        if host in {"localhost", "127.0.0.1", "::1"} and os.path.exists("/.dockerenv"):
            host = "host.docker.internal"
            logger.warning("MYSQL_HOST=localhost no sirve en Docker; se usa host.docker.internal")
        self._params = {
            "host": host,
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "user": user,
            "password": os.getenv("MYSQL_PASSWORD", ""),
            "charset": "utf8mb4",
            "autocommit": True,
            "connect_timeout": 10,
            "read_timeout": 60,
            "write_timeout": 60,
            "cursorclass": DictCursor,
            # Sin esto, un UPDATE que no cambia valores devuelve 0 filas y el candado parecería perdido.
            "client_flag": CLIENT.FOUND_ROWS,
        }
        ssl_ca = os.getenv("MYSQL_SSL_CA")
        if ssl_ca:
            self._params["ssl"] = {"ca": ssl_ca}
        self._local = threading.local()
        self._ensure_schema()

    def _connect(self):
        try:
            return pymysql.connect(database=self.database, **self._params)
        except pymysql.err.OperationalError as exc:
            if exc.args[0] != UNKNOWN_DATABASE:
                raise
        server = pymysql.connect(**self._params)
        try:
            with server.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self.database}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
        finally:
            server.close()
        logger.info("Base de datos MySQL %s creada", self.database)
        return pymysql.connect(database=self.database, **self._params)

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None or not conn.open:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def _drop_conn(self):
        conn = getattr(self._local, "conn", None)
        self._local.conn = None
        if conn is not None:
            try:
                conn.close()
            except pymysql.err.Error:
                pass

    def _execute(self, sql, params=None, fetch=None, many=False):
        for attempt in (1, 2):
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    if many:
                        cur.executemany(sql, params)
                    else:
                        cur.execute(sql, params)
                    if fetch == "one":
                        return cur.fetchone()
                    if fetch == "all":
                        return cur.fetchall()
                    return cur.rowcount
            except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as exc:
                code = exc.args[0] if exc.args else 0
                if attempt == 2 or code not in RECONNECT_ERRORS:
                    raise
                logger.warning("Conexión MySQL perdida (%s); se reconecta", code)
                self._drop_conn()
        return None

    def _ensure_schema(self):
        for statement in SCHEMA:
            self._execute(statement)
        rows = self._execute(
            """
            SELECT DISTINCT table_name AS t, index_name AS i FROM information_schema.statistics
            WHERE table_schema = DATABASE() AND table_name LIKE 'connex\\_%'
            """,
            fetch="all",
        )
        existing = {(row["t"], row["i"]) for row in rows or []}
        for table, name, columns in INDEXES:
            if (table, name) not in existing:
                try:
                    self._execute(f"ALTER TABLE {table} ADD INDEX {name} ({columns})")
                    logger.info("Índice %s creado en %s", name, table)
                except pymysql.err.OperationalError as exc:
                    if exc.args[0] != DUPLICATE_KEY_NAME:
                        raise
        self._execute(
            "INSERT IGNORE INTO connex_lock (name, token, expires_at) VALUES (%s, '', %s)",
            (LOCK_NAME, _utc_naive()),
        )
        self._execute(
            """
            INSERT IGNORE INTO connex_services (service_id, client_id, estado, updated_at)
            SELECT service_id, client_id, %s, UTC_TIMESTAMP()
            FROM connex_circuits
            WHERE state IN (%s, %s, %s)
            """,
            (
                STATUS_DISCONNECTED,
                STATE_DISCONNECTED,
                STATE_DISCONNECT_REQUESTED,
                STATE_RECONNECT_REQUESTED,
            ),
        )

    # --- Circuitos ---

    @staticmethod
    def _circuit_record(row):
        record = {}
        for key, value in row.items():
            if value is None or value == "":
                continue
            record[key] = value.isoformat() if isinstance(value, datetime) else str(value)
        return record

    def get_client_circuits(self, client_id):
        rows = self._execute(
            "SELECT * FROM connex_circuits WHERE client_id = %s", (str(client_id),), fetch="all"
        )
        return {row["service_id"]: self._circuit_record(row) for row in rows or []}

    def client_has_circuits(self, client_id):
        row = self._execute(
            "SELECT 1 AS found FROM connex_circuits WHERE client_id = %s LIMIT 1",
            (str(client_id),),
            fetch="one",
        )
        return bool(row)

    def clients_with_circuits(self):
        rows = self._execute("SELECT DISTINCT client_id FROM connex_circuits", fetch="all")
        return {row["client_id"] for row in rows or []}

    def clients_with_states(self, states):
        states = [str(state) for state in states]
        if not states:
            return set()
        placeholders = ", ".join(["%s"] * len(states))
        rows = self._execute(
            f"SELECT DISTINCT client_id FROM connex_circuits WHERE state IN ({placeholders})",
            tuple(states),
            fetch="all",
        )
        return {row["client_id"] for row in rows or []}

    def set_circuit(self, client_id, service_id, state, **fields):
        unknown = set(fields) - set(CIRCUIT_FIELDS)
        if unknown:
            raise ValueError(f"Campos de circuito desconocidos: {sorted(unknown)}")
        if state == STATE_ACTIVE:
            self.delete_circuit(client_id, service_id)
            return
        values = {
            key: None if fields.get(key) in (None, "") else str(fields[key])
            for key in CIRCUIT_FIELDS
        }
        values["source"] = values["source"] or SOURCE_TICKET
        self._execute(
            """
            INSERT INTO connex_circuits
                (service_id, client_id, state, cid, ticket_id, queue, previous_state,
                 reason, requested_at, source, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                client_id = VALUES(client_id),
                state = VALUES(state),
                cid = VALUES(cid),
                ticket_id = VALUES(ticket_id),
                queue = VALUES(queue),
                previous_state = VALUES(previous_state),
                reason = VALUES(reason),
                requested_at = VALUES(requested_at),
                source = VALUES(source),
                updated_at = VALUES(updated_at)
            """,
            (
                str(service_id),
                str(client_id),
                state,
                values["cid"],
                values["ticket_id"],
                values["queue"],
                values["previous_state"],
                values["reason"],
                values["requested_at"],
                values["source"],
                _utc_naive(),
            ),
        )

    def delete_circuit(self, client_id, service_id):
        self._execute(
            "DELETE FROM connex_circuits WHERE service_id = %s AND client_id = %s",
            (str(service_id), str(client_id)),
        )

    def set_service_status(self, client_id, service_id, estado):
        """El inspector de reconexiones usa esto al dejar un circuito otra vez activo."""
        if estado not in {STATUS_CONNECTED, STATUS_DISCONNECTED}:
            raise ValueError(f"estado inválido: {estado}")
        self._execute(
            """
            INSERT INTO connex_services (service_id, client_id, estado, updated_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                client_id = VALUES(client_id),
                estado = VALUES(estado),
                updated_at = VALUES(updated_at)
            """,
            (str(service_id), str(client_id), estado, _utc_naive()),
        )

    def sync_client_services(self, client_id, rows):
        """
        Inventario del cliente: reemplaza la lista de servicios evaluables.
        rows = [(service_id, estado), ...]. Lo que ya no viene se borra
        (cancelado, FiberX, cargo único).
        """
        client_id = str(client_id)
        wanted = {}
        for service_id, estado in rows:
            if estado not in {STATUS_CONNECTED, STATUS_DISCONNECTED}:
                raise ValueError(f"estado inválido: {estado}")
            wanted[str(service_id)] = estado
        now = _utc_naive()
        if wanted:
            self._execute(
                """
                INSERT INTO connex_services (service_id, client_id, estado, updated_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    client_id = VALUES(client_id),
                    estado = VALUES(estado),
                    updated_at = VALUES(updated_at)
                """,
                [(service_id, client_id, estado, now) for service_id, estado in wanted.items()],
                many=True,
            )
        existing = self._execute(
            "SELECT service_id FROM connex_services WHERE client_id = %s",
            (client_id,),
            fetch="all",
        )
        stale = [row["service_id"] for row in existing or [] if row["service_id"] not in wanted]
        if stale:
            placeholders = ", ".join(["%s"] * len(stale))
            self._execute(
                f"DELETE FROM connex_services WHERE client_id = %s AND service_id IN ({placeholders})",
                (client_id, *stale),
            )

    # --- Pausa tras "Closed Incomplete" ---

    def mark_failed(self, service_id, action, ticket_id, ttl_seconds):
        self._execute(
            """
            INSERT INTO connex_failed (service_id, action, ticket_id, expires_at)
            VALUES (%s, %s, %s, UTC_TIMESTAMP() + INTERVAL %s SECOND)
            ON DUPLICATE KEY UPDATE ticket_id = VALUES(ticket_id), expires_at = VALUES(expires_at)
            """,
            (str(service_id), action, str(ticket_id or ""), max(60, int(ttl_seconds))),
        )

    def recently_failed(self, service_id, action):
        row = self._execute(
            """
            SELECT ticket_id FROM connex_failed
            WHERE service_id = %s AND action = %s AND expires_at > UTC_TIMESTAMP()
            """,
            (str(service_id), action),
            fetch="one",
        )
        return row["ticket_id"] if row else None

    # --- Contrato (una vez visto, se mantiene) ---

    def has_contract(self, client_id):
        row = self._execute(
            "SELECT 1 AS found FROM connex_contract_clients WHERE client_id = %s",
            (str(client_id),),
            fetch="one",
        )
        return bool(row)

    def mark_contract(self, client_id):
        self._execute(
            "INSERT IGNORE INTO connex_contract_clients (client_id, marked_at) VALUES (%s, %s)",
            (str(client_id), _utc_naive()),
        )

    # --- Cachés ---

    def get_invoice_packs(self, invid):
        row = self._execute(
            "SELECT packs FROM connex_invoice_packs WHERE invid = %s AND expires_at > UTC_TIMESTAMP()",
            (str(invid),),
            fetch="one",
        )
        if not row:
            return None
        try:
            return json.loads(row["packs"])
        except json.JSONDecodeError:
            return None

    def cache_invoice_packs(self, invid, packs):
        self._execute(
            """
            INSERT INTO connex_invoice_packs (invid, packs, expires_at)
            VALUES (%s, %s, UTC_TIMESTAMP() + INTERVAL %s SECOND)
            ON DUPLICATE KEY UPDATE packs = VALUES(packs), expires_at = VALUES(expires_at)
            """,
            (str(invid), json.dumps(packs), INVOICE_PACKS_TTL),
        )

    def get_cached_analysis(self, fingerprint):
        row = self._execute(
            """
            SELECT analysis FROM connex_analysis_cache
            WHERE fingerprint = %s AND expires_at > UTC_TIMESTAMP()
            """,
            (fingerprint,),
            fetch="one",
        )
        if not row:
            return None
        try:
            return json.loads(row["analysis"])
        except json.JSONDecodeError:
            return None

    def cache_analysis(self, fingerprint, analysis, ttl_seconds):
        if int(ttl_seconds) <= 0:
            return
        self._execute(
            """
            INSERT INTO connex_analysis_cache (fingerprint, analysis, expires_at)
            VALUES (%s, %s, UTC_TIMESTAMP() + INTERVAL %s SECOND)
            ON DUPLICATE KEY UPDATE analysis = VALUES(analysis), expires_at = VALUES(expires_at)
            """,
            (fingerprint, json.dumps(analysis, ensure_ascii=False), int(ttl_seconds)),
        )

    def purge_expired(self):
        for table in ("connex_invoice_packs", "connex_analysis_cache", "connex_failed"):
            self._execute(f"DELETE FROM {table} WHERE expires_at <= UTC_TIMESTAMP()")

    # --- Historial de tickets pedidos ---

    def record_request(self, record):
        try:
            created_at = _utc_naive(datetime.fromisoformat(str(record.get("created_at"))))
        except ValueError:
            created_at = _utc_naive()
        self._execute(
            """
            INSERT INTO connex_requests (client_id, action, ticket_id, queue, service_ids, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                str(record["client_id"]),
                record["action"],
                str(record.get("ticket_id") or "") or None,
                str(record.get("queue") or "") or None,
                json.dumps(record.get("service_ids") or []),
                created_at,
            ),
        )

    # --- Línea base del primer ciclo ---

    def baseline_started(self):
        row = self._execute(
            "SELECT value FROM connex_meta WHERE name = %s", (BASELINE_META_KEY,), fetch="one"
        )
        return bool(row)

    def start_baseline(self, client_ids):
        """Los clientes de hoy quedan pendientes de línea base; los que aparezcan después, no."""
        rows = [(str(client_id),) for client_id in client_ids]
        if rows:
            self._execute(
                "INSERT IGNORE INTO connex_baseline_pending (client_id) VALUES (%s)", rows, many=True
            )
        self._execute(
            "INSERT IGNORE INTO connex_meta (name, value) VALUES (%s, %s)",
            (BASELINE_META_KEY, datetime.now(timezone.utc).isoformat()),
        )

    def baseline_pending_ids(self):
        rows = self._execute("SELECT client_id FROM connex_baseline_pending", fetch="all")
        return {row["client_id"] for row in rows or []}

    def finish_client_baseline(self, client_id):
        self._execute("DELETE FROM connex_baseline_pending WHERE client_id = %s", (str(client_id),))

    # --- Candado del ciclo ---

    def acquire_lock(self, ttl_seconds):
        token = uuid.uuid4().hex
        acquired = self._execute(
            """
            UPDATE connex_lock
            SET token = %s, expires_at = UTC_TIMESTAMP() + INTERVAL %s SECOND
            WHERE name = %s AND (token = '' OR expires_at <= UTC_TIMESTAMP())
            """,
            (token, max(60, int(ttl_seconds)), LOCK_NAME),
        )
        return token if acquired else None

    def refresh_lock(self, token, ttl_seconds):
        return bool(
            self._execute(
                """
                UPDATE connex_lock
                SET expires_at = UTC_TIMESTAMP() + INTERVAL %s SECOND
                WHERE name = %s AND token = %s AND expires_at > UTC_TIMESTAMP()
                """,
                (max(60, int(ttl_seconds)), LOCK_NAME, token),
            )
        )

    def release_lock(self, token):
        self._execute(
            "UPDATE connex_lock SET token = '', expires_at = UTC_TIMESTAMP() WHERE name = %s AND token = %s",
            (LOCK_NAME, token),
        )
