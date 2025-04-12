from astrapy import DataAPIClient
import json
from collections import defaultdict
from datetime import datetime

with open("config.json") as f:
    config = json.load(f)

client = DataAPIClient(config["ASTRA_DB_TOKEN"])
db = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])

silver = db.get_collection("silver_sales")  # Corrected to get_collection
gold = db.create_collection("gold_monthly_revenue")

monthly = defaultdict(float)

for doc in silver.find({}):
    try:
        dt = datetime.fromisoformat(doc["OrderDate"])
        key = dt.strftime("%Y-%m")
        monthly[key] += doc.get("TotalRevenue", 0)
    except Exception as e:
        print(f"Skipping document due to error: {e}")
        continue

# Check if the document exists and replace it if necessary
for month, rev in monthly.items():
    existing_doc = gold.find_one({"_id": month})  # Find the document with the same _id
    if existing_doc:
        gold.replace_one({"_id": month}, {"_id": month, "TotalRevenue": rev})  # Replace the existing document
    else:
        gold.insert_one({"_id": month, "TotalRevenue": rev})  # Insert a new document if it doesn't exist

print("Gold Table 2: Monthly Revenue created.")
