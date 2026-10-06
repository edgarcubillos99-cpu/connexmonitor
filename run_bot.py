import argparse
import json
import logging

from dotenv import load_dotenv

from core.bot import ConnexBot


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="ConnexMonitor dry-run (solo GET)")
    parser.add_argument("--max-clients", type=int, default=None, help="Limita el escaneo para pruebas")
    parser.add_argument("--client-id", action="append", dest="client_ids", help="Procesa solo estos client_id")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    bot = ConnexBot()

    if args.client_ids:
        summary = {
            "clients_scanned": 0,
            "with_contract": 0,
            "disconnected": 0,
            "overdue_beyond_grace": 0,
            "reconnect_candidates": 0,
            "actions": [],
        }
        for client_id in args.client_ids:
            detail = bot.client.get("client.get", params={"client_id": client_id})
            client_data = detail.get("data") or {"clientid": client_id}
            comments = bot._list_comments(client_id)
            profile = bot._analyze_comments(comments)
            summary["clients_scanned"] += 1
            if profile["has_contract"]:
                summary["with_contract"] += 1
            if profile["is_disconnected"]:
                summary["disconnected"] += 1
                action = bot.handle_disconnected_client(
                    client_id,
                    disconnect_comment=profile["disconnect_comment"],
                )
            else:
                invoices = bot.check_unpaid_invoices(client_id)
                action = bot.handle_overdue_client(
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
            print(
                json.dumps(
                    {
                        "client_id": str(client_id),
                        "has_contract": profile["has_contract"],
                        "is_disconnected": profile["is_disconnected"],
                        "contract_comment": (profile["contract_comment"] or "")[:180],
                        "disconnect_comment": (profile["disconnect_comment"] or "")[:180],
                        "action_type": action.get("type") if action else None,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        print("\n=== RESUMEN ===")
        print(json.dumps({k: v for k, v in summary.items() if k != "actions"}, indent=2))
        print(f"Acciones (dry-run, sin POST): {len(summary['actions'])}")
        return

    summary = bot.process_clients(max_clients=args.max_clients)
    printable = dict(summary)
    printable["actions"] = [
        {
            "type": action.get("type"),
            "client_id": action.get("client_id"),
            "subject": (action.get("ticket") or {}).get("subject"),
        }
        for action in summary["actions"]
    ]
    print(json.dumps(printable, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
