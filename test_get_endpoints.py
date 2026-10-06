import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from core.ubersmith_client import UbersmithClient

REDACT_KEYS = {
    "email",
    "phone",
    "fax",
    "address",
    "ss",
    "login",
    "cpass",
    "password",
}


def _redact(value):
    if isinstance(value, dict):
        return {
            key: ("***" if key.lower() in REDACT_KEYS else _redact(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _print_json(title, payload):
    print(f"\n--- {title} ---")
    print(json.dumps(_redact(payload), indent=2, ensure_ascii=False, default=str))


def _unix_to_iso(value):
    try:
        timestamp = int(value)
        if timestamp <= 0:
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _first_item(data):
    if isinstance(data, dict) and data:
        key = next(iter(data))
        return key, data[key]
    if isinstance(data, list) and data:
        return 0, data[0]
    return None, None


def run_tests():
    load_dotenv()
    try:
        client = UbersmithClient()
    except ValueError as e:
        print(f"Error: {e}")
        print("Llena .env con UBERSMITH_API_URL, UBERSMITH_API_USER y UBERSMITH_API_TOKEN.")
        return

    print("=== Probando API de Ubersmith (solo GET) ===")

    print("\nProbando client.list (limit: 2)...")
    try:
        res = client.get("client.list", params={"limit": 2, "inactive": 0})
        print(f"Status: {res.get('status')}")
        if not res.get("status"):
            print(f"Error API: {res.get('error_message')} (code={res.get('error_code')})")
            return

        data = res.get("data") or {}
        client_id, first_client = _first_item(data)
        if client_id is None:
            print("No se encontraron clientes para continuar las pruebas.")
            return

        print(f"Clientes en esta pagina: {len(data)}")
        print(f"Cliente muestra: ID {client_id}")
        _print_json("client.list (primer cliente, campos redactados)", first_client)
        print("Campos disponibles en client.list:", sorted(first_client.keys()) if isinstance(first_client, dict) else type(first_client))

        print(f"\nProbando client.get para el cliente {client_id}...")
        res_get = client.get("client.get", params={"client_id": client_id})
        print(f"Status: {res_get.get('status')}")
        if res_get.get("status"):
            client_detail = res_get.get("data") or {}
            _print_json(
                "client.get (balance / notas / estado)",
                {
                    "clientid": client_detail.get("clientid"),
                    "active": client_detail.get("active"),
                    "balance": client_detail.get("balance"),
                    "inv_balance": client_detail.get("inv_balance"),
                    "datedue": client_detail.get("datedue"),
                    "grace_due": client_detail.get("grace_due"),
                    "comments": client_detail.get("comments"),
                    "permnote": client_detail.get("permnote"),
                    "tempnote": client_detail.get("tempnote"),
                    "latest_inv": client_detail.get("latest_inv"),
                    "latest_inv_iso": _unix_to_iso(client_detail.get("latest_inv")),
                },
            )

        print(f"\nProbando client.comment_list para el cliente {client_id}...")
        res_comments = client.get(
            "client.comment_list",
            params={"client_id": client_id, "limit": 5, "direction": "desc"},
        )
        print(f"Status: {res_comments.get('status')}")
        comments = res_comments.get("data") or {}
        if res_comments.get("status"):
            print(f"Comentarios obtenidos: {len(comments)}")
            comment_id, first_comment = _first_item(comments)
            if first_comment is not None:
                _print_json(f"client.comment_list (comentario {comment_id})", first_comment)
                if isinstance(first_comment, dict):
                    print("Campos de comentario:", sorted(first_comment.keys()))
                    print("time ISO:", _unix_to_iso(first_comment.get("time")))
                    print("edited ISO:", _unix_to_iso(first_comment.get("edited")))
            else:
                print("Este cliente no tiene comentarios. El bot debera tratarlo como sin contrato.")

        print(f"\nProbando client.invoice_list para el cliente {client_id} (impagas)...")
        res_invoices = client.get(
            "client.invoice_list",
            params={
                "client_id": client_id,
                "paid": 0,
                "limit": 5,
                "order_by": "due",
                "direction": "asc",
            },
        )
        print(f"Status: {res_invoices.get('status')}")
        invoices = res_invoices.get("data") or {}
        if res_invoices.get("status"):
            print(f"Facturas impagas obtenidas: {len(invoices)}")
            invoice_id, first_invoice = _first_item(invoices)
            if first_invoice is not None:
                _print_json(f"client.invoice_list unpaid (factura {invoice_id})", first_invoice)
                if isinstance(first_invoice, dict):
                    print("Campos de factura:", sorted(first_invoice.keys()))
                    print("due raw:", first_invoice.get("due"), "ISO:", _unix_to_iso(first_invoice.get("due")))
                    print("date raw:", first_invoice.get("date"), "ISO:", _unix_to_iso(first_invoice.get("date")))
                    print("paid:", first_invoice.get("paid"), "amount:", first_invoice.get("amount"), "amount_unpaid:", first_invoice.get("amount_unpaid"))
            else:
                print("Este cliente no tiene facturas impagas.")

        print(f"\nProbando client.invoice_list para el cliente {client_id} (pagadas, limit 2)...")
        res_paid = client.get(
            "client.invoice_list",
            params={"client_id": client_id, "paid": 1, "limit": 2, "direction": "desc"},
        )
        print(f"Status: {res_paid.get('status')}")
        if res_paid.get("status"):
            paid = res_paid.get("data") or {}
            print(f"Facturas pagadas obtenidas: {len(paid)}")
            _, first_paid = _first_item(paid)
            if isinstance(first_paid, dict):
                print("paid:", first_paid.get("paid"), "datepaid raw:", first_paid.get("datepaid"), "ISO:", _unix_to_iso(first_paid.get("datepaid")))

        print("\n=== Pruebas GET finalizadas. No se ejecuto ningun POST. ===")

    except Exception as e:
        print(f"Error en la peticion: {e}")


if __name__ == "__main__":
    run_tests()
