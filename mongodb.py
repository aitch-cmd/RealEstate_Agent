import os
import sys
import pymongo
import certifi
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = "rental_database"
MONGODB_URL = os.getenv("MONGODB_URL_KEY")  # Directly get the MongoDB URI

# Load the certificate authority file to avoid timeout errors when connecting to MongoDB
ca = certifi.where()

class MongoDBClient:
    """
    MongoDBClient is responsible for establishing a connection to the MongoDB database.
    """

    client = None  # Shared MongoClient instance

    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        try:
            # If client hasn't been created yet, create it
            if MongoDBClient.client is None:
                if MONGODB_URL is None:
                    raise Exception("Environment variable 'MONGODB_URL' is not set.")
                
                # Create a new client
                MongoDBClient.client = pymongo.MongoClient(MONGODB_URL, tlsCAFile=ca)

            # Use shared client
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name

        except Exception as e:
            raise Exception(e, sys)

