import pymongo
from datetime import datetime

# MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["JBMEMS"]
collection = db["EMSDB23"]

# Define the time range
start_time = datetime.strptime("2023-08-16T15:04:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
end_time = datetime.strptime("2023-08-16T15:07:30.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")

# Fetch data within the time range
# Fetch data within the time range
query = {
    "time": {"$gte": start_time, "$lte": end_time},
    "kWh_ABS223": {"$exists": True}
}
data = list(collection.find(query))  # Convert cursor to a list

# Print the number of fetched documents
print("Number of fetched documents:", len(data))

# Print fetched data in terminal
for item in data:
    print("Fetched data:", item)
