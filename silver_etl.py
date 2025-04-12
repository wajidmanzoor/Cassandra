from astrapy import DataAPIClient
import json
from datetime import datetime

with open("config.json") as f:
    config = json.load(f)

client = DataAPIClient(config["ASTRA_DB_TOKEN"])
db = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])

bronze = db.collection("bronze_sales")
silver = db.create_collection("silver_sales")

records = bronze.find({})

cleaned = []
for doc in records:
    try:
        doc["OrderDate"] = datetime.strptime(doc["Order Date"], "%m/%d/%Y").isoformat()
        doc["ShipDate"] = datetime.strptime(doc["Ship Date"], "%m/%d/%Y").isoformat()
        doc["UnitsSold"] = int(doc["Units Sold"])
        doc["UnitPrice"] = float(doc["Unit Price"])
        doc["TotalProfit"] = float(doc["Total Profit"])
        doc["TotalRevenue"] = float(doc["Total Revenue"])
        doc["_id"] = str(doc.get("Order ID", ""))
        cleaned.append(doc)
    except:
        continue

for doc in cleaned:
    silver.insert_one(doc)

print("Silver ETL complete.")