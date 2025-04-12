from astrapy import DataAPIClient
import json
from collections import defaultdict

with open("config.json") as f:
    config = json.load(f)

client = DataAPIClient(config["ASTRA_DB_TOKEN"])
db = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])

silver = db.get_collection("silver_sales")  # Corrected to get_collection
gold = db.create_collection("gold_channel_comparison")

channel = defaultdict(float)

for doc in silver.find({}):
    try:
        # Ensure 'Sales Channel' key exists in the document
        ch = doc.get("Sales Channel")  # Use the correct column name here
        if ch:
            channel[ch] += doc.get("TotalRevenue", 0)
        else:
            print(f"Skipping document with no 'Sales Channel' key or empty value. Document ID: {doc.get('_id')}")
    except Exception as e:
        print(f"Skipping document due to error: {e}. Document ID: {doc.get('_id')}")
        continue

# Insert the calculated revenue per channel into the gold table
for ch, rev in channel.items():
    gold.insert_one({"_id": ch, "TotalRevenue": rev})

print("Gold Table 3: Channel Revenue Comparison created.")
