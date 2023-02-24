from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
from datetime import datetime as dt

load_dotenv(find_dotenv())

password = os.environ.get("MONGO_DB_PWD")

string = f"mongodb+srv://mugami:{password}@kindredcluster.izmyqif.mongodb.net/?retryWrites=true&w=majority&authSource=admin"
client = MongoClient(string)

printer = pprint.PrettyPrinter()

production = client.production # database

student_validator= {
      "$jsonSchema": {
         "bsonType": "object",
         "required": [ "name", "year", "major", "address" ],
         "properties": {
            "name": {
               "bsonType": "string",
               "description": "must be a string and is required"
            },
            "year": {
               "bsonType": "int",
               "minimum": 2017,
               "maximum": 3017,
               "description": "must be an integer in [ 2017, 3017 ] and is required"
            },
            "major": {
               "enum": [ "Math", "English", "Computer Science", "History", "null" ],
               "description": "can only be one of the enum values and is required"
            },
           "gpa": {
               "bsonType": [ "double" ],
               "description": "must be a double if the field exists"
            },
            "address": {
               "bsonType": "object",
               "required": [ "city" ],
               "properties": {
                  "street": {
                     "bsonType": "string",
                     "description": "must be a string if the field exists"
                  },
                  "city": {
                     "bsonType": "string",
                     "description": "must be a string and is required"
                  }
               }
            }
         }
      }
   }

try:
    production.create_collection("student")
except Exception as e:
    printer.pprint(e)

production.command("collMod", "student", validator=student_validator)

def create_teacher_collection():
    teacher_validator= {
      "$jsonSchema": {
         "bsonType": "object",
         "required": [ "name", "year", "major", "address" ],
         "properties": {
            "name": {
               "bsonType": "string",
               "description": "must be a string and is required"
            },
            "year": {
               "bsonType": "int",
               "minimum": 2017,
               "maximum": 3017,
               "description": "must be an integer in [ 2017, 3017 ] and is required"
            },
            "major": {
               "enum": [ "Math", "English", "Computer Science", "History", "null" ],
               "description": "can only be one of the enum values and is required"
            },
            "address": {
               "bsonType": "object",
               "required": [ "city" ],
               "properties": {
                  "street": {
                     "bsonType": "string",
                     "description": "must be a string if the field exists"
                  },
                  "city": {
                     "bsonType": "string",
                     "description": "must be a string and is required"
                  }
               }
            }
         }
      }
   }
    try:
        production.create_collection("teachers")
    except Exception as e:
        printer.pprint(e)

    production.command("collMod", "teachers", validator=teacher_validator)


#bulk insert
def create_data():
    teachers = [
        {
        "name": "Alex Fregurson",
        "year": 2017,
        "major":"Math",
        "address":{
            "city":"Nairobi"
        }
        },
        {
        "name": "Arsene Wenger",
        "year": 2018,
        "major":"English",
        "address":{
            "city":"Mombasa"
        }
        },
        {
        "name": "Pep Guardiola",
        "year": 2021,
        "major":"History",
        "address":{
            "city":"Kisumu"
        }
        }
    ]

    teacher_collection = production.teachers
    teachers_ids = teacher_collection.insert_many(teachers).inserted_ids

    students = [
        {
        "name": "Wayne Rooney",
        "year": 2020,
        "major":"Math",
        "teacher": teachers_ids[0],
        "address":{
            "city":"Nairobi"
        }
        },
        {
        "name": "Thierry Henry",
        "year": 2019,
        "major":"English",
        "teacher": teachers_ids[1],
        "address":{
            "city":"Mombasa"
        }
        },
        {
        "name": "Sergio aguero",
        "year": 2021,
        "major":"History",
        "teacher": teachers_ids[2],
        "address":{
            "city":"Kisumu"
        }
        }
    ]

    student_collection = production.student
    student_collection.insert_many(students)


####

students_containing_a = production.student.find({"name": {"$regex": "a{1}"}})#only 1 a

#printer.pprint(list(students_containing_a))

teachers_and_students = production.teachers.aggregate([{
    "$lookup":{
        "from":"student",
        "localField":"_id" ,#field on teachers
        "foreignField":"teacher",
        "as":"students"
    }
}])###left join

##printer.pprint(list(teachers_and_students))

teachers_students_count = production.teachers.aggregate([
    {
    "$lookup":{
        "from":"student",
        "localField":"_id" ,#field on teachers
        "foreignField":"teacher",
        "as":"students"
    }
    },
    {
        "$addFields":{
            "total_students":{"$size":"$students"}
        }
    },
    {
        "$project":{"name":1, "total_students":1, "major":1, "_id":0}
    }
    ])

##printer.pprint(list(teachers_students_count))
# students_with_old_teachers = production.student.aggregate([
#     {
#         "$lookup":{
#             "from":"teachers",
#             "localField":"teacher",
#             "foreignField":"_id",
#             "as":"teachers"
#         }
#     },
#     {
#         "$set":{
#             "teachers":{
#                 "$map":{
#                     "input": "$teachers",
#                     "in":{
#                         "year":{
#                             "$dateDiff":{
#                                 "startDate":"$$this.year",
#                                 "endDate": "$$NOW",
#                                 "unit":"year"
#                             }
#                         },
#                         "name":"$$this.name",
#                         "major":"$$this.major",
#                     }
#                 }
#             }
#         }
#     },
#     {
#         "$match":{
#             "$and":[
#                 {"teachers.year": {"$gte": 1}},
#                 {"teachers.year": {"$lte": 5}}
#             ]
#         }
#     }, 
#     {
#         "$sort":{
#             "year": 1
#         }
#     }
# ])

#printer.pprint(list(students_with_old_teachers)) year not a date hence raised error

import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
import pymongoarrow as pma
from bson import ObjectId

patch_all()

teacher = Schema({"_id": ObjectId, "name": pyarrow.string(), "year": int})

df = production.teachers.find_pandas_all({}, schema =teacher)

print(df.head())

