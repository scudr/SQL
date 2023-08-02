''' #! /usr/bin/python '''

import pymongo
from bson.son import SON

print("Setting up mongo client")

client = pymongo.MongoClient("localhost", 27017)

db = client['tutorialDB']

collection = db['employees']


#Insert Documents
# employee = {
#     "name": "John Doe",
#     "age": 30,
#     "department": "Sales",
#     "position": "Sales Manager",
#     "years_of_experience": 5
# }

# result = collection.insert_one(employee)
# print("Document inserted with id", result.inserted_id)


# Find one document
doc = collection.find_one()
print(doc)


#Find all documents
docs = collection.find()
for doc in docs:
    print(doc)


#Find with a filter
docs = collection.find({"department": "Sales"})
for doc in docs:
    print(doc)


#Update one document
# result = collection.update_one({"name":"John Doe"}, {"$set":{"age":31}})
# print(result.modified_count, "documents updated.")

# #update many documents
# result = collection.update_many({"department": "Sales"}, {"$set": {"department": "Marketing"}})
# print(result.modified_count, "documents updated.")
# print("Connecting to database ...")



# Delete many documents
# result = collection.delete_many({"department": "Marketing"})
# print(result.deleted_count, "documents deleted.")


#Aggregation
pipeline=[
    {"$group":{"_id": "$department", "avgExperience":{"$avg": "$years_of_experience"}}},
    {"$sort": SON([("avgExperience",-1)])}
]

avg_experiences = list(db.employees.aggregate(pipeline))
for doc in avg_experiences:
    print(doc)


# Indexes
# result = db.employees.create_index([("age", pymongo.ASCENDING)])
# print("index created with name", result)

# Find all employees older than 30

older_than_30 = db.employees.find({"age":{"$gt":30}})

# print the employees
for employee in older_than_30:
    print(employee)