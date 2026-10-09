import json
from dotenv import load_dotenv
from core.ubersmith_client import UbersmithClient

load_dotenv()
client = UbersmithClient()

# Get some clients
clients_payload = client.get_paginated("client.list", params={"limit": 5, "active": 1})
clients = clients_payload.get("data", {})

for client_id in list(clients.keys())[:5]:
    services_payload = client.get_paginated("client.service_list", params={"client_id": client_id, "pack_type_select": 4})
    services = services_payload.get("data", {})
    for packid, srv in services.items():
        if isinstance(srv, dict) and str(srv.get("suspend_bool")) == "1":
            print(f"Client {client_id} has suspended service {packid}")
            print(json.dumps(srv, indent=2))
            break
