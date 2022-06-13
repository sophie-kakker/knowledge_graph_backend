import logging
import pymongo
import uuid


class MongoLogger:
    def __init__(self, url, database, collection):
        self.client = pymongo.MongoClient(url)
        self.db = self.client[database]
        self.collection = self.db[collection]

    def push_sample_relations(self, relations):
        ingestion_id = str(uuid.uuid4())
        if len(relations) > 10:
            relations = relations[:10]
        data = {"ingestion_id": ingestion_id,
                "relations": relations}
        self.collection.insert_one(data)
        logging.info(f"Sample data logged for ingestion_id: {ingestion_id}")
        return ingestion_id

