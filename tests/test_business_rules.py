import threading
import time
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from core.bot import ConnexBot, apply_business_rules
from core.openai_analyzer import redact_personal_data

TODAY = date(2026, 10, 8)


def invoice(due, amount="44.95"):
    return {
        "invid": "1",
        "amount_unpaid": amount,
        "due_date": due,
        "days_overdue": max(0, (TODAY - due).days) if due else 0,
    }


class BusinessRulesTest(unittest.TestCase):
    def test_disconnect_without_overdue_invoice_is_cancelled(self):
        analysis = {"action": "disconnect", "has_contract": False}
        action, reason = apply_business_rules(
            analysis, [invoice(date(2026, 10, 15))], Decimal("44.95"), TODAY
        )
        self.assertEqual(action, "none")
        self.assertEqual(reason, "sin_facturas_vencidas")

    def test_disconnect_without_contract_has_no_grace(self):
        analysis = {"action": "disconnect", "has_contract": False}
        action, reason = apply_business_rules(
            analysis, [invoice(date(2026, 10, 7))], Decimal("44.95"), TODAY
        )
        self.assertEqual(action, "disconnect")
        self.assertIsNone(reason)

    def test_contract_grace_lasts_61_days_from_oldest_due(self):
        analysis = {"action": "disconnect", "has_contract": True}
        # 8 ago + 61 días = 8 oct; todavía dentro del máximo.
        within = apply_business_rules(
            analysis, [invoice(date(2026, 8, 8)), invoice(date(2026, 9, 15))], Decimal("90"), TODAY
        )
        self.assertEqual(within, ("none", "prorroga_contrato_hasta_2026-10-08"))
        # 7 ago + 61 días = 7 oct; el 8 oct ya se pasó.
        beyond = apply_business_rules(
            analysis, [invoice(date(2026, 8, 7))], Decimal("44.95"), TODAY
        )
        self.assertEqual(beyond, ("disconnect", None))

    def test_disconnect_with_zero_balance_is_cancelled(self):
        analysis = {"action": "disconnect", "has_contract": False}
        action, _ = apply_business_rules(analysis, [invoice(date(2026, 1, 1))], Decimal("0"), TODAY)
        self.assertEqual(action, "none")

    def test_reconnect_requires_paid_account(self):
        analysis = {"action": "reconnect"}
        self.assertEqual(
            apply_business_rules(analysis, [invoice(date(2026, 9, 1))], Decimal("44.95"), TODAY),
            ("none", "deuda_pendiente"),
        )
        self.assertEqual(apply_business_rules(analysis, [], Decimal("0"), TODAY), ("reconnect", None))


class RedactionTest(unittest.TestCase):
    def test_hides_personal_data_but_keeps_context(self):
        text = (
            "Cliente cambia email a juan.perez@yahoo.com, tarjeta VISA ****6440 05/24, "
            "ACH E6333019344002411, tel 787-555-1234. Debe $69.90, Ticket #282731"
        )
        redacted = redact_personal_data(text)
        for secret in ("juan.perez@yahoo.com", "6440", "6333019344002411", "787-555-1234"):
            self.assertNotIn(secret, redacted)
        for kept in ("$69.90", "#282731", "05/24"):
            self.assertIn(kept, redacted)


class TicketSubjectTest(unittest.TestCase):
    def test_staff_subjects_match_the_right_action(self):
        disconnect = {"subject": "Desconectar / Pagan Rivera, Edwin X Client ID: 56104"}
        reconnect = {"subject": "Reconectar / Pagan Rivera, Edwin X / Client ID: 56104"}
        self.assertTrue(ConnexBot._ticket_matches_action(disconnect, "disconnect"))
        self.assertFalse(ConnexBot._ticket_matches_action(disconnect, "reconnect"))
        self.assertTrue(ConnexBot._ticket_matches_action(reconnect, "reconnect"))
        self.assertFalse(ConnexBot._ticket_matches_action(reconnect, "disconnect"))


class FakeStore:
    def acquire_lock(self, ttl):
        return "token"

    def refresh_lock(self, token, ttl):
        return True

    def release_lock(self, token):
        return None

    def get_cached_analysis(self, fingerprint):
        return None

    def cache_analysis(self, fingerprint, analysis, ttl):
        return None

    def should_skip(self, *args, **kwargs):
        return False, "sin_registro"


class ConcurrentAnalyzer:
    def __init__(self):
        self.current = 0
        self.max_seen = 0
        self.lock = threading.Lock()

    def analyze(self, client_id, comments, services, invoices, balance, balance_bucket, today):
        with self.lock:
            self.current += 1
            self.max_seen = max(self.max_seen, self.current)
        time.sleep(0.05)
        with self.lock:
            self.current -= 1
        return {
            "action": "none",
            "has_contract": False,
            "reason": "test",
            "service_ids": [],
            "confidence": 1.0,
        }


class ConcurrencyTest(unittest.TestCase):
    def test_process_clients_runs_several_at_once(self):
        analyzer = ConcurrentAnalyzer()
        fake_client = type("Client", (), {"page_size": 50})()
        bot = ConnexBot(client=fake_client, store=FakeStore(), analyzer=analyzer)
        bot.concurrency = 4
        clients = {str(i): {"clientid": str(i), "balance": "0"} for i in range(8)}

        def fake_comments(client_id):
            return []

        def fake_services(client_id):
            return []

        def fake_invoices(client_id, today):
            return []

        with (
            patch.object(bot, "_load_clients", return_value=clients),
            patch.object(bot, "_list_comments", side_effect=fake_comments),
            patch.object(bot, "_list_services", side_effect=fake_services),
            patch.object(bot, "_list_unpaid_invoices", side_effect=fake_invoices),
        ):
            summary = bot.process_clients()

        self.assertEqual(summary["clients_scanned"], 8)
        self.assertEqual(summary["skipped_none"], 8)
        self.assertGreaterEqual(analyzer.max_seen, 2)


if __name__ == "__main__":
    unittest.main()
