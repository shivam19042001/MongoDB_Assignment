#1 Write a Python script to load the Superstore dataset from a CSV file into MongoDB.

import pandas as pd
from pymongo import MongoClient
import pprint

# === Step 1: Load the Superstore CSV ===
try:
    superstore_df = pd.read_csv("superstore.csv", encoding='ISO-8859-1')
    print("✅ CSV file loaded successfully.")
except Exception as e:
    print(f" Failed to load CSV: {e}")
    exit()

# === Step 2: Convert DataFrame to list of dictionaries ===
records = superstore_df.to_dict(orient='records')

# === Step 3: Connect to MongoDB ===
try:
    mongo_uri = "mongodb://localhost:27017/"
    client = MongoClient(mongo_uri)

    # Connect to database and collection
    db = client["SuperstoreDB"]
    collection = db["Orders"]

    # === Step 4: Insert data into MongoDB ===
    result = collection.insert_many(records)
    print(f"✅ {len(result.inserted_ids)} records inserted successfully!")

except Exception as e:
    print(f" Failed to connect or insert data: {e}")

# 2. Retrieve and print all documents
print("\n#2. All Documents:")
for doc in collection.find():
    pprint.pprint(doc)

# 3. Count total documents
print("\n#3. Total number of documents:")
count = collection.count_documents({})
print(f"Total: {count}")

# 4. Orders from 'West' region
print("\n#4. Orders from West region:")
for doc in collection.find({"Region": "West"}):
    pprint.pprint(doc)

# 5. Orders with Sales > 500
print("\n#5. Orders with Sales > 500:")
for doc in collection.find({"Sales": {"$gt": 500}}):
    pprint.pprint(doc)

# 6. Top 3 orders by Profit
print("\n#6. Top 3 orders by Profit:")
for doc in collection.find().sort("Profit", -1).limit(3):
    pprint.pprint(doc)

# 7. Update Ship Mode from "First Class" to "Premium Class"
print("\n#7. Updating Ship Mode...")
result = collection.update_many(
    {"Ship Mode": "First Class"},
    {"$set": {"Ship Mode": "Premium Class"}}
)
print(f"{result.modified_count} documents updated.")

# 8. Delete orders with Sales < 50
print("\n#8. Deleting orders with Sales < 50...")
result = collection.delete_many({"Sales": {"$lt": 50}})
print(f"{result.deleted_count} documents deleted.")

# 9. Aggregation: Total Sales per Region
print("\n#9. Total Sales per Region:")
pipeline = [
    {"$group": {"_id": "$Region", "TotalSales": {"$sum": "$Sales"}}}
]
for result in collection.aggregate(pipeline):
    pprint.pprint(result)

# 10. Distinct Ship Modes
print("\n#10. Distinct Ship Modes:")
ship_modes = collection.distinct("Ship Mode")
print(ship_modes)

# 11. Count of orders per Category
print("\n#11. Order count per Category:")
pipeline = [
    {"$group": {"_id": "$Category", "OrderCount": {"$sum": 1}}}
]
for result in collection.aggregate(pipeline):
    pprint.pprint(result)