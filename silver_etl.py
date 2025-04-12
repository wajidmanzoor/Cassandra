from astrapy import DataAPIClient
import json
from datetime import datetime

with open("config.json") as f:
    config = json.load(f)

client = DataAPIClient(config["ASTRA_DB_TOKEN"])
db = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])

bronze = db.get_collection("bronze_sales")  # Corrected to get_collection
silver = db.create_collection("silver_sales")

records = bronze.find({})

cleaned = []
for doc in records:
    try:
        # Correcting column names according to the provided CSV headers
        doc["OrderDate"] = datetime.strptime(doc["Order Date"], "%m/%d/%Y").isoformat()
        doc["ShipDate"] = datetime.strptime(doc["Ship Date"], "%m/%d/%Y").isoformat()
        doc["UnitsSold"] = int(doc["UnitsSold"])
        doc["UnitPrice"] = float(doc["UnitPrice"])
        doc["TotalProfit"] = float(doc["TotalProfit"])
        doc["TotalRevenue"] = float(doc["TotalRevenue"])
        
        # Using OrderID as the unique identifier
        doc["_id"] = str(doc["Order ID"])  
        
        cleaned.append(doc)
    except Exception as e:
        print(f"Skipping document due to error: {e}")
        continue

# Insert cleaned data into the silver collection
for doc in cleaned:
    silver.insert_one(doc)

print("Silver ETL complete.")
