import json
from dotenv import load_dotenv
from core.ubersmith_client import UbersmithClient

load_dotenv()
client = UbersmithClient()

payload = client.get_paginated(
    "client.service_list",
    params={"client_id": "22453", "pack_type_select": 4, "metadata": 1}
)

services = payload.get("data", {})
print(json.dumps(services, indent=2, default=str))
