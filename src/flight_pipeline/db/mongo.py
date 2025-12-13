from pymongo import MongoClient
from pymongo.database import Database

from flight_pipeline.config.settings import mongo_settings


def get_mongo_client() -> MongoClient:
    """
    Create and return a MongoDB client connected to the replica set.
    """
    return MongoClient(mongo_settings.uri)


def get_database() -> Database:
    """
    Return the main application database.
    """
    client = get_mongo_client()
    return client[mongo_settings.database]
