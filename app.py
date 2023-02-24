from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())

password = os.environ.get("MONGO_DB_PWD")

string = f"mongodb+srv://mugami:{password}@kindredcluster.izmyqif.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(string)

printer = pprint.PrettyPrinter()

# dbs = client.list_database_names()
test_db = client.Test
# collections = test_db.list_collection_names()


def insert_test_doc():
    collection = test_db.Test
    document = {
        "name": "Brian Mugami",
        "Type": "User"
    }
    id = collection.insert_one(document).inserted_id
    print(id)


production = client.production
person_collection = production.person_collection


def create_documents():
    first_names = ["Brian", "Leroy", "Alvin"]
    last_names = ["Mugami", "Lusenaka", "Musengezi"]
    ages = [24, 25, 56]

    docs = []
    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {
            "first_name": first_name,
            "last_name": last_name,
            "age": age
        }

        docs.append(doc)

    person_collection.insert_many(docs)


def find_all_people():
    people = person_collection.find()
    for person in people:
        printer.pprint(person)


def find_person(name: str):
    person = person_collection.find_one({"first_name": name})
    printer.pprint(person)


def couunt_all_people():
    count = person_collection.count_documents(filter={})
    print(f"number of people {count}")


def find_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})

    printer.pprint(person)


def get_age_range(min: int, max: int):
    query = {"$and": [
        {"age": {"$gte": min}},
        {"age": {"$lte": max}}
    ]
    }

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)


def project_columns():
    columns = {"_id": 0, "first_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)


def update_person_by_id(_id: str):
    from bson.objectid import ObjectId

    _id = ObjectId(_id)

    # all_updates = {
    #     "$set":{"new_field": True},
    #     "$inc":{"age": 1},
    #     "$rename": {"first_name": "fname", "last_name":"lname"}
    # }

    # person_collection.update_one({"_id":_id}, all_updates)

    updates = {
        "$unset": {"new_field": ""},
    }
    person_collection.update_one({"_id": _id}, updates)


def replace_one(_id: str):
    from bson.objectid import ObjectId

    _id = ObjectId(_id)

    new_doc = {
        "fname": "Brayo", "lname": "muga", "age": 100
    }
    person_collection.replace_one({"_id": _id}, new_doc)


def delete_doc_by_id(_id):
    from bson.objectid import ObjectId

    _id = ObjectId(_id)
    person_collection.delete_one({"_id": _id})  # deletemany


# ---------------------------------------------------------------------------------

address = {
    "_id":"526372376276273563765ffsdf",
    "name": "Ngong",
    "city":"Nairobi"
}

def add_address_embed(_id, address):
    from bson.objectid import ObjectId

    _id = ObjectId(_id)

    updates = {
        "$addToSet":{"address": address}
    }

    person_collection.update_one({"_id": _id}, updates)


def add_address_relationship(_id, address):
    from bson.objectid import ObjectId

    _id = ObjectId(_id)

    address = address.copy()
    address["owner_id"] = _id

    address_collection = production.address

    address_collection.insert_one(address)

####

