An anonimized example of reading mongo via python

```python
import datetime
import pymongo
import psycopg2
from bson import ObjectId

client = pymongo.MongoClient('mongodb://--:--@sm1.somenergia.lan:27017/somenergia')
db_mongo = client.somenergia
db_mongo.giscedata_profiles_profile.find_one()
import datetime
import pymongo
import psycopg2
from bson import ObjectId

client = pymongo.MongoClient('mongodb://--:--@sm1.somenergia.lan:27017/somenergia')
db_mongo = client.somenergia
db_mongo.giscedata_profiles_profile.find_one()
db_mongo.tg_f1.find_one()
cursor = db.tg_cchfact.find({"name": "ES0122000011123456CE0F", "create_at" : {"$gt": datetime.datetime.strptime("2017-09-13T09:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}})
cursor = db_mongo.tg_cchfact.find({"name": "ES0122000011123456CE0F", "create_at" : {"$gt": datetime.datetime.strptime("2017-09-13T09:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}})
cursor.count()
db_mongo.tg_cchfact.find_one({"name": "ES0122000011123456CE0F", "create_at" : {"$gt": datetime.datetime.strptime("2017-09-13T09:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}})
db_mongo.profiles_profile.find_one({"name": "ES0122000011123456CE0F", "create_at" : {"$gt": datetime.datetime.strptime("2017-09-13T09:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}})
db_mongo.giscedata_profiles_profile.find_one()
db_mongo.profiles_profile.find_one({"create_date" : {"$gt": datetime.datetime.strptime("2017-09-13T09:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}})
db_mongo.profiles_profile.find_one({"create_date" : {"$gt": datetime.datetime.strptime("2021-09-13T09:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}})
db.giscedata_profiles_profile.find( { timestamp: { $gt: ISODate("2018-01-01T01:00:00Z")}} ).count()
db.giscedata_profiles_profile.find( { timestamp: { $gt: datetime.datetime.strptime("2018-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).count()
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2018-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).count()
db = db_mongo
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2018-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).count()
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).count()
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort({_id:-1}).limit(1)
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort({"_id":-1}).limit(1)
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort("_id", -1).limit(1)
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort("_id", -1).limit(1)
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort("_id", -1).limit(1).fetch_one()
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort("_id", -1).limit(1).fetchone()
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort("_id", -1).limit(1)
foo = db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort("_id", -1).limit(1)
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort({"_id":-1, pymongo.DESCENDING}).limit(1)
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort({"_id":-1, pymongo.ASCENDING}).limit(1)
db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort({"_id", pymongo.ASCENDING}).limit(1)
foo = db.giscedata_profiles_profile.find( { "timestamp": { "$gt": datetime.datetime.strptime("2023-01-01T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}} ).sort("_id", -1).limit(1)
query = {
            'datetime': {
                 '$gte': di, #datetime('2018-08-01T01:00'),
                 '$lte': df #datetime('2018-08-02T03:00')
            },
            'name': {
                '$in': [
                    'ES00311035123456HE0F'
                ]
            }
        }
query = {
            'datetime': {
                 '$gte': datetime('2018-08-01T01:00'),
                 '$lte': datetime('2018-08-02T03:00')
            },
            'name': {
                '$in': [
                    'ES00311035123456HE0F'
                ]
            }
        }
query = {
            'datetime': {
                 '$gte': datetime.datetime('2018-08-01T01:00'),
                 '$lte': datetime.datetime('2018-08-02T03:00')
            },
            'name': {
                '$in': [
                    'ES00311035123456HE0F'
                ]
            }
        }
query = {
            'datetime': {
                 '$gte': datetime.datetime(2018,8,1,1),
                 '$lte': datetime.datetime(2018,8,2,3)
            },
            'name': {
                '$in': [
                    'ES00311035123456HE0F'
                ]
            }
        }
db_mongo.profiles_profile.find_one({"cups": "ES0122000011123456CE0F", "create_at" : {"$gt": datetime.datetime.strptime("2017-09-13T09:00:00Z", "%Y-%m-%dT%H:%M:%SZ")}})
db_mongo.profiles_profile.find_one().sort(pymongo.DESCENDING)
db_mongo.profiles_profile.find_one()
db.profiles_profile.find_one()
db_mongo.giscedata_profiles_profile.find_one()
db.giscedata_profiles_profile.find_one().sort(pymongo.DESCENDING)
db.giscedata_profiles_profile.sort(pymongo.DESCENDING).find_one()
db.profiles_profile.find_one()
db_mongo.giscedata_profiles_profile.find_one()
db_mongo.giscedata_profiles_profile.find_one({}, sort=[('create_date', pymongo.DESCENDING)]))
db_mongo.giscedata_profiles_profile.find_one({}, sort=[('create_date', pymongo.DESCENDING)])
db_mongo.giscedata_profiles_profile.find().limit(10)
list(db_mongo.giscedata_profiles_profile.find().limit(10))
history
%history -f mongo_ipython.txt
```