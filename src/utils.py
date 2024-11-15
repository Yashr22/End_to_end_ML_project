import pymongo
from pymongo import MongoClient
from src.exception import CustomException
from src.logger import logging
import sys

def get_database_connection(uri: str, database_name: str):
    """
    Establishes a connection to the MongoDB database and returns the database object.
    
    Parameters:
        uri (str): MongoDB connection URI.
        database_name (str): Name of the database to connect to.
    
    Returns:
        Database object.
    """
    try:
        # Establish MongoDB connection
        client = MongoClient(uri)
        db = client[database_name]
        logging.info(f"Connected to MongoDB: {database_name}")
        return db
    except Exception as e:
        logging.error("Error connecting to MongoDB.")
        raise CustomException(e, sys)