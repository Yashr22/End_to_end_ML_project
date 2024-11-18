import pandas as pd
import numpy as np
import dill
import os
import pickle

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
    

def save_object(file_path, obj):
    try:
        dir_path = os.path.dirname(file_path)

        os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)

    except Exception as e:
        raise CustomException(e, sys)