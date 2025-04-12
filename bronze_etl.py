import pandas as pd
from astrapy import DataAPIClient
import json

with open("config.json") as f:
    config = json.load(f)

client = DataAPIClient(config["ASTRA_DB_TOKEN"])
db = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])

bronze = db.create_collection("bronze_sales")

df = pd.read_csv("./data/bronze_sales.csv")
print("CSV Columns:", df.columns.tolist())
docs = df.to_dict(orient="records")

for doc in docs:
    doc["_id"] = str(doc.get("Order ID", ""))
    bronze.insert_one(doc)

print("Bronze ETL complete.")