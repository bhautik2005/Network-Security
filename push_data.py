import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

Mongo_URI = os.getenv("MONGODB_URI")
print(Mongo_URI)


import certifi
ca = certifi.where()
 
import pymongo
import numpy as np
import pandas as pd
from Networkscurity.exception.execption import NetworkSecurityException
from Networkscurity.logging.logger import logging 


class NetworkDataExtract():
  
    def __init__(self):
        try:
            logging.info("Trying to connect to MongoDB")
            self.client = pymongo.MongoClient(Mongo_URI, tlsCAFile=ca)
            logging.info("Successfully connected to MongoDB")
        except Exception as e:
            raise NetworkSecurityException(e,sys)
          
    def csv_to_json(self,file_path:str)->str:
        try:
             data=pd.read_csv(file_path)
             data.reset_index(inplace=True,drop=True)
             records = list(json.loads(data.T.to_json()).values())
             return records
        except  Exception as e:
            raise NetworkSecurityException(e,sys)
          
    def insert_data_mongodb(self,records,database,collections):
      try:
          self.database = database
          self.collection = collections
          self.records = records
          self.mongo_clinet = pymongo.MongoClient(Mongo_URI)
          
          self.database = self.mongo_clinet[self.database]
          self.collection = self.database[self.collection]
          self.collection.insert_many(records)
          return(len(records))
          logging.info("Data inserted successfully into MongoDB")
       
      except Exception as e:
          raise NetworkSecurityException(e,sys)
        
if __name__=='__main__':
    FILE_PATH ="Network_Data\phisingData.csv"
    DATABASE="bhautik"
    Collection="NetworkSecurity"
    networdObject=NetworkDataExtract()
    records = networdObject.csv_to_json(file_path=FILE_PATH)
    print(records)
    no_of_records = networdObject.insert_data_mongodb(records=records,database=DATABASE,collections=Collection)
    
    print(f"{no_of_records} records inserted successfully into MongoDB")