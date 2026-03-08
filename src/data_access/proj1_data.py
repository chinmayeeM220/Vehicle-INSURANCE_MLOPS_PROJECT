import sys
import pandas as pd
import numpy as np
from typing import Optional

from src.configuration.mongo_db_connection import MongoDBClient
from src.constants import DATABASE_NAME
from src.exception import MyException

class Proj1Data:
    """
    Ye class MongoDB se data nikal kar use DataFrame mein badalti hai.
    """
    def __init__(self):
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise MyException(e, sys)

    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        try:
            # Database connection set karna
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client.client[database_name][collection_name]

            # 1. MongoDB se data fetch karna
            df = pd.DataFrame(list(collection.find()))

            # 2. "_id" column ko drop karna (FIXED LINE NEECHE HAI)
            # Humne axis=1 hata diya hai kyunki columns use kar rahe hain
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"]) 

            # 3. Agar koi "id" naam ka column hai toh use bhi hata sakte hain
            if "id" in df.columns.to_list():
                df = df.drop(columns=["id"])

            # 4. Missing values (na) ko NaN se replace karna
            df.replace({"na": np.nan}, inplace=True)

            return df

        except Exception as e:
            raise MyException(e, sys)