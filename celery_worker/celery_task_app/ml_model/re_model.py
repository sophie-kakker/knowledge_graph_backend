import logging

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
from ..knowledge_base import knowledge_base
from ..kg_ingestor import kg_ingestor
from ..mongo_logger import mongo_logger
import math
import torch


class RelationExtractionModel:
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("Babelscape/rebel-large", cache_dir="celery_task_app/ml_model"
                                                                                           "/model_cache")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("Babelscape/rebel-large", cache_dir="celery_task_app"
                                                                                               "/ml_model/model_cache")
        self.gen_kwargs = {
            "max_length": 256,
            "length_penalty": 0,
            "num_beams": 3,
            "num_return_sequences": 3,
        }
        scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
        host_name = "neo4j_container"
        port = 7687
        url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
        user = "neo4j"
        password = "test"
        self.kg_ingestor = kg_ingestor.KGIngestor(url, user, password)
        logging.info("connected to KG ingestor successfully")
        self.mongo_logger = mongo_logger.MongoLogger("mongodb://mongodb_container:27017/", "ingestion_db",
                                                     "ingestion_logs")
        logging.info("connected to mongo logger successfully")

    def process_data(self, text: str, verbose=True):
        # Plan
        # Create KB out of it
        # Iterate over KB to ingest relations
        kb = self.from_text_to_kb(text)
        relations = kb.relations
        count = 0
        ingestion_id = self.mongo_logger.push_sample_relations(relations)
        for relation in relations:
            count += 1
            self.kg_ingestor.create_relationship(relation['head'], relation['type'], relation['tail'])
        if verbose:
            print(f"{count} relationships have been ingested")

        return {"ingestion_id": ingestion_id}

    def from_text_to_kb(self, text, span_length=128, verbose=False):
        # tokenize whole text
        inputs = self.tokenizer([text], return_tensors="pt")

        # compute span boundaries
        num_tokens = len(inputs["input_ids"][0])
        if verbose:
            print(f"Input has {num_tokens} tokens")
        num_spans = math.ceil(num_tokens / span_length)
        if verbose:
            print(f"Input has {num_spans} spans")
        overlap = math.ceil((num_spans * span_length - num_tokens) /
                            max(num_spans - 1, 1))
        spans_boundaries = []
        start = 0
        for i in range(num_spans):
            spans_boundaries.append([start + span_length * i,
                                     start + span_length * (i + 1)])
            start -= overlap
        if verbose:
            print(f"Span boundaries are {spans_boundaries}")

        # transform input with spans
        tensor_ids = [inputs["input_ids"][0][boundary[0]:boundary[1]]
                      for boundary in spans_boundaries]
        tensor_masks = [inputs["attention_mask"][0][boundary[0]:boundary[1]]
                        for boundary in spans_boundaries]
        inputs = {
            "input_ids": torch.stack(tensor_ids),
            "attention_mask": torch.stack(tensor_masks)
        }
        generated_tokens = self.model.generate(
            **inputs,
            **self.gen_kwargs,
        )

        # decode relations
        decoded_preds = self.tokenizer.batch_decode(generated_tokens,
                                                    skip_special_tokens=False)
        if verbose:
            print(f"Decoded outputs length {len(decoded_preds)}")

        # create kb
        kb = knowledge_base.KnowledgeBase()
        i = 0
        for sentence_pred in decoded_preds:
            current_span_index = i // self.gen_kwargs["num_return_sequences"]
            relations = self.extract_relations_from_model_output(sentence_pred)
            for relation in relations:
                relation["meta"] = {
                    "spans": [spans_boundaries[current_span_index]]
                }
                kb.add_relation(relation)
            i += 1

        return kb

    @staticmethod
    def extract_relations_from_model_output(text):
        relations = []
        relation, subject, relation, object_ = '', '', '', ''
        text = text.strip()
        current = 'x'
        text_replaced = text.replace("<s>", "").replace("<pad>", "").replace("</s>", "")
        for token in text_replaced.split():
            if token == "<triplet>":
                current = 't'
                if relation != '':
                    relations.append({
                        'head': subject.strip(),
                        'type': relation.strip(),
                        'tail': object_.strip()
                    })
                    relation = ''
                subject = ''
            elif token == "<subj>":
                current = 's'
                if relation != '':
                    relations.append({
                        'head': subject.strip(),
                        'type': relation.strip(),
                        'tail': object_.strip()
                    })
                object_ = ''
            elif token == "<obj>":
                current = 'o'
                relation = ''
            else:
                if current == 't':
                    subject += ' ' + token
                elif current == 's':
                    object_ += ' ' + token
                elif current == 'o':
                    relation += ' ' + token
        if subject != '' and relation != '' and object_ != '':
            relations.append({
                'head': subject.strip(),
                'type': relation.strip(),
                'tail': object_.strip()
            })
        return relations
