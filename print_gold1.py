from astrapy import DataAPIClient
import json

with open("config.json") as f:
    config = json.load(f)

client = DataAPIClient(config["ASTRA_DB_TOKEN"])
db = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])

for name in ["gold_top_countries", "gold_monthly_revenue", "gold_channel_comparison"]:
    col = db.get_collection(name)  # fixed .collection -> .get_collection
    print(f"\nData from {name}:")
    for doc in col.find({}, limit=5):  # fixed invalid options argument
        print(doc)
    print("\n\n")