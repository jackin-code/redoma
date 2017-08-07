#!/usr/bin/python
import threading, logging, time, json,pymongo,sys,random,datetime
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from collections import deque
import requests

# Global variables
mongo_client= None
rdmapping = None
db = None
extraInstanceNo = "extra-1001"
meta_mongo_url = "mongodb:27017"
meta_mongo_client = None
# Hack: workaround the sequence problem for initial loading
retry_queue = deque([])

def loadConfig():
    #if len(sys.argv) < 2: 
    #    print "Usage: python rdm.py <relational-document-mapping-file>"
    #    exit()
    global meta_mongo_client
    global extraInstanceNo
    rawconfig = None
    jobname = None
    if len(sys.argv) >= 2: 
        jobname = sys.argv[1]

    
    if (jobname and ".json" in jobname ):
        print("### Loading job from file")
        with open(jobname) as json_data:
            rawconfig = json.load(json_data)
            rawconfig["jobfile"] = jobname
            print("Parsed configuration: "+str(rawconfig))
    else:
        # lookup in redoma/jobs collection
        # TODO: mongodb should be configurable
        # print("### Loading job from "+meta_mongo_url)
        if(not meta_mongo_client):
            meta_mongo_client = MongoClient(meta_mongo_url)
        meta_db = meta_mongo_client["redoma"]
        meta_coll= meta_db["jobs"]
        if(jobname):
            print("Loading job from mongodb by name "+jobname)
            rawconfig = meta_coll.find_one({"_id": jobname})        
            print(rawconfig)
            if(rawconfig == None):
                print("Failed to load job from mongodb, check localhost:27017/redmoa/jobs collection")
                exit()
        else:
            rawconfig = meta_coll.find_one_and_update(
                {"$or": [{"state": "running", "extraInstanceNo": None}, {"state": "running", "extraInstanceNo": extraInstanceNo}]},
                {"$set": {"state": "running", "extraInstanceNo": extraInstanceNo}})
            if(rawconfig):
                print("Found job")
                print("rawconfig")
            

    return rawconfig

def initConfig(rawconfig):
    rdmapping = rawconfig 

    # convert underscore to dot, database_host -> database.host
    updated_config = {}
    for key,value in rdmapping["config"].iteritems():
        key = key.replace("_", ".")
        updated_config[key]=value

    # mongo.database
    if("mongo.database" not in updated_config):
        if("_id" in rdmapping):
            updated_config["mongo.database"] = rdmapping["_id"].replace(" ","_").replace(".", "_")

    # generate a random database.server.id, this is required by mysql replication to 
    # uniquely identify the source in a cluster
    # if database.type == "mysql" then: 
    updated_config["database.server.id"] = str(random.randint(10000000,90000000))

    #   database.server.name": "dbserver-mysql", 
    # use database.hostname instead
    updated_config["database.server.name"] = updated_config["database.host"] 
    updated_config["database.hostname"] = updated_config["database.host"] 

    rdmapping["config"] = updated_config

    # restructure the tables 
    tables = rdmapping["tables"]
    table_list = []
    if(not isinstance(tables, list)):
        for table_name, mappings in tables.iteritems():
            mappings["table"] = table_name
            table_list.append(mappings)
        rdmapping["tables"] = table_list
        print("Restructured tables: ", table_list)

    return rdmapping

def configureMessageQueue(rdmapping):
    if( "testdatra" in rdmapping):
        return
    print ("## Configuring mongo message queue")

def configureTopics(rdmapping):
    if( "testdata" in rdmapping):
        return
    print("### Configuring kafka topic")
    import requests,json
    kafka_api_endpoint = "http://connect:8083/connectors/"
    payload={}
    payload["name"] = rdmapping["config"]["database.name"]+"-connector"
    payload["config"] = {
        "connector.class" : "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max" : "1",
        "database.hostname" : "mysql",
        "database.port" : "3306",
        "database.user" : "debezium",
        "database.password" : "dbz",        
        "database.history.kafka.bootstrap.servers" : "kafka:9092",

        #"database.history.kafka.topic" : "dbhistory.inventory"      
        #"database.whitelist" : "inventory",
    }
    payload["config"]["database.history.kafka.topic"]= "dbhistory."+rdmapping["config"]["database.name"]
    payload["config"]["database.whitelist"]=rdmapping["config"]["database.name"]

    # generate a random database.server.id, this is required by mysql replication to 
    # uniquely identify the source in a cluster
    # if database.type == "mysql" then: 
    #payload["config"]["database.server.id"] = str(random.randint(10000000,90000000))
    #   database.server.name": "dbserver-mysql", 
    payload["config"].update(rdmapping["config"])

    # use database.hostname instead
    #payload["config"]["database.server.name"] = payload["config"]["database.host"] 
    
    # update the global variable
    #rdmapping["config"] = payload["config"]

    hd={"Content-Type":"application/json", "Accept": "application/json"}
    print("Configuring topics")
    r = requests.post(kafka_api_endpoint, data=json.dumps(payload), headers=hd)
    print("Result: " + r.text)  
    if("error_code" in str(r.text)):
        pass   #TODO handle the error
    else:
        time.sleep(5)


def initialize(rdmapping):        
    global db
    global mongo_client
    print("### Initializing")
    # configure logging
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

    mongo_uri = "mongodb:27017" 
    if(rdmapping["config"]["mongo.uri"]):
        mongo_uri = rdmapping["config"]["mongo.uri"]

    # connect to mongodb
    print("Connecting to "+mongo_uri)
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client[rdmapping["config"]["mongo.database"]]
    #for table in rdmapping["tables"]: print(table["table"] +" -> "+ table["collection"])    

# {u'tables': [{u'fields': {u'first_name': u'first_name', u'last_name': u'last_name', u'id': u'id', u'email': u'email'},
# u'insertionType': u'insert', u'collection': u'customers', u'table': u'customers'}], u'state': u'start-requested', u'_id': u'jackin',
# u'config': {u'database.port': 1521.0, u'database.type': u'oracle', u'database.username': u'LOGMINER', u'mongo.uri': u'mongodb://mongodb:27017',
# 'database.server.id': '12591375', u'mongoUri': u'mongodb://jackinhuang.com:27017', u'mongo.database': u'inventory', 'database.hostname': u'192.168.71.43',
# u'database.name': u'XE', u'database.host': u'192.168.71.43', u'database.password': u'LOGMINER', 'database.server.name': u'192.168.71.43'},
# u'source': {u'products_on_hand': {u'pk': [u'product_id'], u'fields': [u'quantity', u'product_id']}, u'customers': {u'pk': [u'id'], u'fields':
# [u'first_name', u'last_name', u'id', u'email']}, u'products': {u'pk': [u'id'], u'fields': [u'description', u'name', u'weight', u'id']},
# u'orders': {u'pk': [u'order_number'], u'fields': [u'order_date', u'quantity', u'product_id', u'purchaser', u'order_number']}}}
def createConsumer(rdmapping):
    print rdmapping
    meta_db = meta_mongo_client["redoma"]
    coll_name = "message_"+ rdmapping["_id"] + "_" + rdmapping["config"]["database.type"] + "_" + rdmapping["config"]["database.hostname"].replace(".", "_") + "_" + rdmapping["config"]["database.name"]
    coll = meta_db[coll_name]
    return coll

# def createConsumer(rdmapping):
#     print("### Create kafka subscriber")
#     #print (rdmapping["config"])
#     if("testdata" in rdmapping ):
#         return rdmapping["testdata"]
#     kafka_uri="kafka:9092"
#     if("kafka.uri" in rdmapping["config"]):
#         kafka_uri = rdmapping["config"]["kafka.uri"]
#
#     consumer = KafkaConsumer(bootstrap_servers= kafka_uri, auto_offset_reset='earliest')
#     topics=[]
#     for table in rdmapping["tables"]:
#         topic = rdmapping["config"]["database.server.name"]+"."+rdmapping["config"]["database.name"]+"."+ table["table"]
#         if(topic not in topics):
#             topics.append(topic)
#     consumer.subscribe(topics)
#     print("### Subscribed topics: "+str(topics))
#     return consumer
    #consumer.subscribe(['dbserver1.inventory.customers', 'dbserver1.inventory.orders', 'dbserver1.inventory.products','dbserver1.inventory.products_on_hand'])

def upsert(coll, table_config, doc, is_upsert):
    v = doc["value"].get("after", doc["value"].get("before"))
    print(v)
    query={}

    op = doc["value"].get("op", None)
    if(is_upsert):
        condition = table_config["condition"]
        if (isinstance(condition, unicode) or isinstance(condition, str)):
            condition = json.loads(condition)
        query = {}
        for key, value in condition.iteritems():
            #print(key, value)
            if("$" in value ):
                value = value.replace("$","")
                value = v[value]        
            query[key] = value
        v = {"$set": v} 
        print ("CONDITION: " , condition, "  QUERY: ", query, " Value: ", v)
        result = coll.update_one(query, v, upsert=True)
        if(result and result.upserted_id):
            print("Upserted ",result.upserted_id)
    else:
        if op == "d":
            result = coll.delete_many(v)
        else:
            result = coll.insert_one(v)

    return result

def embedOne(coll, table_config, doc):
    '''
    {
        "table":        "products",
        "collection":   "orders",
        "contdition":   {"product_id": "$id"},
        "insertionType":  "embedOne",
        "embedPath":   "product_detail"                
    }   
    '''
    op = doc.get("op", None)
    v = doc["value"].get("after", doc["value"].get("before"))
    query={}    
    embedPath = table_config["table"] # by default use the table name as embedPath
    if("embedPath" in table_config):
        embedPath = table_config["embedPath"]
    condition = table_config["condition"]
    if (isinstance(condition, unicode) or isinstance(condition, str)):
        condition = json.loads(condition)
    
    # process the substitutions  {product_id: "$id"}    
    # todo: a more general purpose substitution framework
    query={}
    for key, value in condition.iteritems():
        print (key, value)
        if("$" in value ):
            value = value.replace("$","")
            value = v[value]        
        query[key] = value
    print ("CONDITION: " , condition, "  QUERY: ", query)
    doc = {}
    doc[embedPath] = v
    #TODO: make multi:True UI configurable
    if op == "d":
        result = coll.update_many(query, {"$pull": {embedPath: v}})
    else:
        result = coll.update_many(query, {"$set": doc }, upsert=True)

    if(result and result.upserted_id):
        print("Upserted ",result.upserted_id)
    '''
    if(result and result.matched_count == 0):
        # hack, lets wait and retry it later
        doc["retry"] = doc["retry"] + 1 if "retry" in doc else 1
        retry_queue.append(doc)
        print("Retry #", doc["retry"])
    '''
    #TODO: handle error case
    return "OK"

def embedMany(coll, table_config, doc):
    '''
    {
        "table":        "orders",
        "collection":   "customers",                
        "insertionType":       "embedMany",                
        "condition":   {"id": "$purchaser"},
        "embedPath":   "orders",
    }    
    '''
    op = doc["value"].get("op", None)
    v = doc["value"].get("after", doc["value"].get("before"))
    query={}    
    embedPath = table_config["table"] # by default use the table name as embedPath
    if("embedPath" in table_config):
        embedPath = table_config["embedPath"]
    condition = table_config["condition"]
    if (isinstance(condition, unicode) or isinstance(condition, str)):
        condition = json.loads(condition)
    
    # process the substitutions  {product_id: "$id"}    
    # todo: a more general purpose substitution framework
    query={}
    for key, value in condition.iteritems():
        print (key, value)
        if("$" in value ):
            value = value.replace("$","")
            value = v[value.lower()]
        query[key] = value
    print ("CONDITION: " , condition, "  QUERY: ", query)
    if op == "d":
        result = coll.update_many(query, {"$pull": {embedPath: v}})
    elif op == "u":
        result = updateDocument(coll, embedPath, query, v)
    else:
        result = coll.update_many(query, {"$push": {embedPath: v}}, upsert=True)
    if(result and result.upserted_id):
        print("Upserted ",result.upserted_id)
    '''
    print("push result: matched ", result.matched_count, result.modified_count, result.raw_result)
    if(result and result.matched_count == 0):
        # hack, lets wait and retry it .ater
        doc["retry"] = doc["retry"] + 1 if "retry" in doc else 1
        retry_queue.append(doc)
        print("Retry #", doc["retry"])
    '''
    #TODO: handle error case
    return "OK"

# def processMessage(message, rdmapping):

def updateDocument(coll, embedPath, query, v):
    insertValue = {}
    if (isinstance(v, unicode) or isinstance(v, str)):
        v = json.loads(v)
    if not embedPath:
        query[embedPath + "." + "_id"] = v["_id"]
        for key, value in v.iteritems():
            key = embedPath + ".$." + key
            insertValue[key] = value
        result =coll.update_many(query, {"$set": insertValue})
        return result


def processMessage(message, rdmapping):
    print ("\n"+str(datetime.datetime.now())+" ############### RAW MSG #################\n")
    print(message)
    if(isinstance(message, dict)):
        message = DictAttr(message)
    topic = message.tableName.split(".")
    tname = topic.pop()
    table_config = None

    cdc_coll = db["cdc_change_log"]

    doc={}
    doc["topic"]=message.tableName
    # doc["timestamp"]=message.timestamp
    # doc["offset"] = message.offset
    #doc["key"] = message.key
    if(isinstance(message, dict)):
        doc["value"] = message
    else:
        doc["value"]=json.loads(message)

    result = cdc_coll.insert_one(doc)
    if result:
        print("Inserted raw: " , result.inserted_id)

    retvar = {"inserted":0, "updated":0, "deleted":0 }
    for tc in rdmapping["tables"]:
        if tc["table"].lower() != tname.lower():
            continue
        #print("Table config ", tc)
        
        table_config = tc                  
        collname = table_config["collection"]
        print ("target "+ collname)
        coll = db[collname]
        

        action="insert"
        if("action" in table_config):
            action = table_config["action"]            
        if("insertionType" in table_config):
            action = table_config["insertionType"]
        
        # embedOne, embedMany
        if doc["value"] and doc["value"].get("after", doc["value"].get("before")):
            result = None
            if(action == "insert" ):
                result = upsert(coll, table_config, doc, False)
                retvar["inserted"] += 1               
            if(action == "upsert" or action == "merge"):
                result = upsert(coll, table_config, doc, True)                
                retvar["updated"] += 1
            #if(action == "push-to-array" ):
            #    result = pushToArray(coll, table_config, doc)
            if(action == "embedOne"):
                result = embedOne(coll, table_config, doc)
                retvar["updated"] += 1
            if(action == "embedMany"):
                result = embedMany(coll, table_config, doc)
                retvar["updated"] += 1
            if result: 
                print("Updated record: ",result)

        #print "Before:  ", (doc["value"]["payload"]["before"])
        #print "After: ",  (doc["value"]["payload"]["after"])   

    if not table_config:
        print ("Table not defined: "+tname+". Aborting message" )

    return retvar
        
class Consumer(threading.Thread):
    daemon = True
    counters = {"raw":0, "inserted":0, "updated":0, "deleted":0}    
    def updateCounters():
        None
    def run(self):        
        global meta_mongo_client
        
        while(True):
            rawconfig = loadConfig()            
            if(rawconfig == None):
                if( len(sys.argv)>=2):
                    exit()
                else:
                    #print("Trying again in one second")
                    time.sleep(1)
                    continue
            print("Retrieved raw config")
            rdmapping = initConfig(rawconfig)
            initialize( rdmapping)
            #configureMessageQueue(rdmapping)
            consumer = createConsumer(rdmapping)                

            while(True):
                message = consumer.find_one_and_update({"status": "untreated"}, {"$set": {"status": "treated"}})
                if not message:
                    time.sleep(1)
                    continue
                ret = processMessage(message, rdmapping)


            lastsave = self.counters["raw"]
            for message in consumer:
                # Hack: process queued retries
                #if(len(retry_queue)>0):
                #    msg = retry_queue.popleft() 
                #    processMessage(msg, rdmapping)
                ret = processMessage(message,rdmapping) 

                self.counters["raw"]+=1
                self.counters["inserted"]+= ret["inserted"]
                self.counters["updated"]+= ret["updated"]
                self.counters["deleted"]+= ret["deleted"]
                if(self.counters["raw"] - lastsave > 0): 
                    # persist, TBD: based on time interval
                    if(not meta_mongo_client):
                        meta_mongo_client = MongoClient(meta_mongo_url)
                    meta_db = meta_mongo_client["redoma"]
                    meta_coll= meta_db["jobs"]
                    meta_coll.update_one({"_id": rawconfig["_id"]}, {"$set":{"counters": self.counters}}, upsert=True)

            if("jobfile" in rawconfig):
                break

def main():
    print("Version 0.0.1")    
    #rdmapping = loadConfig()
    #initialize( rdmapping)
    threads = [ 
        # Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(3600)


class DictAttr(dict):
    def __getattr__(self, key):
        if key not in self:
            raise AttributeError(key)
        return self[key]
 
    def __setattr__(self, key, value):
        self[key] = value
 
    def __delattr__(self, key):
        del self[key]

if __name__ == "__main__":        
    main()



'''

def pushToArray(coll, table_config, doc):
    
    {
            "table":        "orders",
            "collection":   "customers",
            "condition":   {"id": "$purchaser"}
            "insertionType":       "embedOne",
            "embedPath":         "orders"
    },
    v = doc["value"]["payload"]["after"]
    query={}    
    arrayPath = table_config["table"]
    if("path" in table_config):
        arrayPath = table_config["path"]
    keys= table_config["lookup-keys"]
    while(len(keys)>0):
        target_key = keys.pop();
        current_key = keys.pop();        
        query[target_key] = v[current_key]
    print "Query: "+str(query)
    result = coll.update_one(query, {"$push":{arrayPath:v}})    
    return "OK"
'''








