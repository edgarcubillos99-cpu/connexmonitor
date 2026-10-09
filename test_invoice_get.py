import json
from dotenv import load_dotenv
from core.ubersmith_client import UbersmithClient

load_dotenv()
client = UbersmithClient()

payload = client.get(
    "client.invoice_get",
    params={"invoice_id": "852718"}
)

print(json.dumps(payload, indent=2, default=str))
