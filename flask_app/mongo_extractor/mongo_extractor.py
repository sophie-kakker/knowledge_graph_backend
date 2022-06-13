import logging
import pymongo


class MongoExtractor:
    def __init__(self, url, database, collection):
        self.client = pymongo.MongoClient(url)
        self.db = self.client[database]
        self.collection = self.db[collection]

    def get_sample_relations(self, task_id):
        query = {"task_id": task_id}
        results = self.collection.find(query)
        docs = []
        for doc in results:
            docs.append(doc)
        if len(docs) == 0:
            return {}
        return docs[0]

    def update_doc(self, task_id, ingestion_id):
        op = self.collection.update_one({'ingestion_id': ingestion_id}, {"$set": {"task_id": task_id}}, upsert=False)
        if op.matched_count == 0:
            logging.error(f"failed to find any match for task_id: {task_id}")
        else:
            logging.info(f"updated task_id {task_id} for ingestion_id {ingestion_id}")