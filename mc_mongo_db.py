#upsert
#An option for update operations; e.g. db.collection.updateOne(), db.collection.findAndModify().
# If set to true, the update operation will either update the document(s) matched by the specified query or if no documents match, insert a new document. 

#CRUD :
#Create
#db.collection.insertOne() 
#db.collection.insertMany()

#read 
#db.collection.find()

#Delete 

#db.collection.deleteOne() New in version 3.2

#db.collection.deleteMany() 

#Aggregation pipelines, which are the preferred method for performing aggregations.

#    Group values from multiple documents together.

#Perform operations on the grouped data to return a single result.

# Analyze data changes over time.
# $match
# $size
#https://www.geeksforgeeks.org/upsert-in-mongodb/
# upsert is an option that is used for update operation, update + insert = upsert

#findAndModify() 
# update() 
#Upsert with Aggregation Pipeline:


from pymongo import MongoClient
import re
from pprint import pprint 
import datetime
from bson import ObjectId
client = MongoClient(
    host="XX.XXX.XXX",
    port = 27017
)

print(client.list_database_names())

print(client['sample'].list_collection_names ())


#print(list(client['sample']['city'].find({}).limit(1)))
print(client['sample']['city'].find_one())




#doc = col.find_one_and_update(
 #   {"_id" : ObjectId("5cfbb46d6fb0f3245fd8fd34")},
 #   {"$set":
 #       {"some field": "OBJECTROCKET ROCKS!!"}
  #  },upsert=True
#)

#client['sample']['city'].delete_one({'_id': ObjectId('56d61033a378eccde8a8354f')})

pprint(client['sample']['city'].find( {'_id': ObjectId('56d61033a378eccde8a8354f')}))

post = { 'id': '10021-2015-ENFO', 'certificate_number': 9278806, 'business_name': 'ATLIXCO DELI GROCERY INC.',
 'date': 'Feb 20 2015', 'result': 'No Violation Issued', 'sector': 'Cigarette Retail Dealer - 127', 
'address': {'city': 'RIDGEWOOD', 'zip': 11385, 'street': 'MENAHAN ST', 'number': 1712}}

#post_id = client['sample']['city'].insert_one(post).inserted_id

#print(post_id)

pprint(list(client['sample']['city'].find( {'_id': ObjectId('63e43020bd9bc3253d16b356')})))

client['sample']['city'].update_one(
    {'_id': ObjectId('63e43020bd9bc3253d16b356')},{
  '$set': {
    'address.city':'RIDGEWOOD_NO',
    'city':"None'"
  }
}, upsert=False)
pprint(list(client['sample']['city'].find( {'_id': ObjectId('63e43020bd9bc3253d16b356')})))

# we set the value of this option is true, then the method performs one of the following operations:
# If a document or documents found that matches the given query criteria, then the update() method updates the document/documents.
#If no document/documents match the given query criteria, then the update() method inserts a new document in the collection.

#client['sample']['city'].update_many(
  #  {'business_name': 'Cool Company'},
    
  #  {
  #'$set': {
   # 'address.city':'city Random',
    #'address.street':'Street Alea',
    #'address.number':'1',
  # 'adresse.zip':'nnnn',
    #"date" : 'Feb 09 2023'
 # }
#, upsert=True)


pprint(list(client['sample']['city'].find( {'business_name': 'Cool Business'})))


#db.employee.update({name:”Ram”}, [{"$set": {"department": "HR", "age":30}}],{upsert:true})


pprint(list(client['sample']['city'].aggregate([
   {"$match": {"$or" : 
      [
        { "address.street":{ "$regex" :"BLVD"}}, 
     { "address.street":{ "$regex" :"PL"}}, 

      ]
    }
  }, 
  {"$project" : {
  "business_name":1,
  'result':1,
  "sector": { "$regexMatch": { "input": "$sector", "regex": r'[a-zA-z]+' }},
  "full_adress": {"$concat" :  [ {"$convert":{
         "input": "$address.zip",
         "to":  "string"  }}," ", {"$toString": "$address.number"}," ", "$address.street"," ","$address.city"]},

"Date_form":
         { "$dateFromString": {
     "dateString": "$date",
     "timezone": "America/New_York"
    
} },

"Day": { "$dayOfWeek":   { "$dateFromString": {
     "dateString": "$date",
     "timezone": "America/New_York"
    
} }}

   }}
  ]
)))

#  "full_adress": {
 #   "$reduce": {
   #    "input": [ "address.zip","$address.number", "address.street","$address.city"],
    #   "initialValue": "",
    #   "in": { "$concat" : ["$$value", "$$this"] }
    # }
 #}