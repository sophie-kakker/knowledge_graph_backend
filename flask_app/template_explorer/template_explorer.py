import json
import logging
from elasticsearch import Elasticsearch
import hashlib
import re


class ElasticTemplateExplorer:
    def __init__(self, kg_explorer, elastic_url="http://localhost:9200"):
        self.es_cluster = Elasticsearch([elastic_url])
        logging.info("Elastic cluster client created")
        logging.info(self.es_cluster.info())
        self.create_template_index()
        self.clear_index()
        self.relation_list = None
        #self.get_index_size()
        self.kg_explorer = kg_explorer
        logging.info("connected to KG explorer successfully")
        self.ingest_standard_templates()

    def get_relation_list(self):
        if self.relation_list is None:
            self.relation_list = []
            with open("resources/triplets_dedup.txt",mode='r',encoding='utf-8') as f:
                for line in f:
                    self.relation_list.append(line.strip())
        return self.relation_list

    def clear_index(self, name="template_store"):
        self.es_cluster.indices.delete(index=name, ignore=[400, 404])
        logging.info(f"Deleting index {name}")

    def create_template_index(self, name="template_store"):
        if not self.es_cluster.indices.exists(index=name):
            self.es_cluster.indices.create(index=name)

    def ingest_standard_templates(self,filepath="resources/standard_templates.jsonl"):
        with open(filepath, mode='r',encoding='utf-8') as f:
            count = 0
            for line in f:
                if len(line)<10:
                    return
                relation_obj = json.loads(line.strip('\n'))
                relation = relation_obj['relation']
                for template in relation_obj['templates']:
                    count += 1
                    self.ingest_template(relation, template['pattern'], template['groups'])
        logging.info(f"{count} standard templates ingested")

    def ingest_template(self, relation, template, groups=None, index="template_store"):
        if groups is None:
            groups = []
        doc = {"relation": relation, "template": template, "groups":groups}
        doc_id = hashlib.md5(template.encode('utf-8')).hexdigest()
        self.es_cluster.index(index=index, body=doc, id=doc_id)

    def search_template(self, query: str, index="template_store"):
        query = query.lower()
        search_query = {
            "from": 0,
            "size": 2,
            "query": {
                "match": {
                    "template": query
                }
            }
        }
        res = self.es_cluster.search(index=index, body=search_query)
        if len(res['hits']['hits']) > 0:
            template_obj = res['hits']['hits'][0]['_source']
            entity = self.extract_entity(template_obj['template'], template_obj['groups'], query)
            answer = self.kg_explorer.find_relation_tail(entity, template_obj['relation'])
            return {"answer": answer}
        return {"message": "no matching template found"}

    def extract_entity(self, template,groups, query):
        p = re.compile(template)
        if groups is None or len(groups) == 0:
            entity = p.findall(query)[0]
        else:
            entity = p.findall(query)[0][groups[0]]
        return entity.strip()

    def get_index_size(self, index="template_store"):
        logging.info(f"Current index size {self.es_cluster.cat.count(index=index)}")
