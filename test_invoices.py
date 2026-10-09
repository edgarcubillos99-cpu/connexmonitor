import json
from dotenv import load_dotenv
from core.ubersmith_client import UbersmithClient

load_dotenv()
client = UbersmithClient()

payload = client.get_paginated(
    "client.invoice_list",
    params={"client_id": "22453", "paid": 0}
)

invoices = payload.get("data", {})
print(json.dumps(invoices, indent=2, default=str))
