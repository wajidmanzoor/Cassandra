from astrapy import DataAPIClient
import json
from collections import defaultdict

with open("config.json") as f:
    config = json.load(f)

client = DataAPIClient(config["ASTRA_DB_TOKEN"])
db = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])

silver = db.get_collection("silver_sales")  # Corrected to get_collection
gold = db.create_collection("gold_top_countries")

country_profit = defaultdict(float)

for doc in silver.find({}):
    country = doc["Country"]
    country_profit[country] += doc.get("TotalProfit", 0)

top = sorted(country_profit.items(), key=lambda x: x[1], reverse=True)[:5]

for country, profit in top:
    gold.insert_one({"_id": country, "TotalProfit": profit})

print("Gold Table 1: Top Countries created.")
