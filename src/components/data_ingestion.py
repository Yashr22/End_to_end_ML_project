import os
import sys
from src.exception import CustomException
from src.logger import logging
from src.utils import get_database_connection
import pandas as pd

from sklearn.model_selection import train_test_split
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    train_data_path : str=os.path.join('artifacts','train.csv')
    test_data_path : str=os.path.join('artifacts','test.csv')
    raw_data_path : str=os.path.join('artifacts','raw.csv')

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        try:
            # MongoDB Connection Details
            mongo_uri = "mongodb://localhost:27017"  # Replace with your MongoDB URI
            db_name = "MyDatabase"  # Replace with your database name
            collection_name = "Student"  # Replace with your collection name

            # Get database connection
            db = get_database_connection(mongo_uri, db_name)
            collection = db[collection_name]

            logging.info("Fetching data from MongoDB collection.")

            # Fetch all documents and convert to a DataFrame
            data = list(collection.find())
            df = pd.DataFrame(data)

            # Drop the MongoDB `_id` field if it exists
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])

            logging.info(f"Data fetched from database. Shape: {df.shape}")

            # Save raw data to CSV
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)
            df.to_csv(self.ingestion_config.raw_data_path, index=False)
            logging.info("Raw data saved to CSV.")

            # Split data into train and test sets
            train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)
            train_set.to_csv(self.ingestion_config.train_data_path, index=False)
            test_set.to_csv(self.ingestion_config.test_data_path, index=False)

            logging.info("Data ingestion completed successfully.")
            return (
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
        except Exception as e:
            logging.error("Error occurred during data ingestion.")
            raise CustomException(e, sys)
        
if __name__=='__main__':
    obj=DataIngestion()
    obj.initiate_data_ingestion()