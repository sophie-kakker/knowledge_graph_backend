from py2neo import Graph, Node, Relationship, NodeMatcher, RelationshipMatcher, GraphService
import logging
import json
import re


class KGExplorer:
    def __init__(self, url, user, password):
        self.graph_service = GraphService(url, auth=(user, password))
        logging.info("graph service initiated successfully")
        self.default_graph = self.graph_service.default_graph
        self.default_node_matcher = NodeMatcher(self.default_graph)
        self.default_relation_matcher = RelationshipMatcher(self.default_graph)

    def transform_relation(self, relation):
        return str(relation).replace(" ", "_")

    def rev_transform_relation(self,relation):
        return str(relation).replace("_", " ")

    def _get_node(self, node_name, node_label="ENTITY", graph_name=None):
        if graph_name is not None:
            node_matcher = self.get_node_matcher(graph_name=graph_name)
        else:
            node_matcher = self.default_node_matcher
        node_match = node_matcher.match(node_label, name=node_name)
        if node_match.exists():
            return node_match.first(), True
        else:
            return None, False

    def get_node_matcher(self, graph_name):
        graph = self.graph_service[graph_name]
        return NodeMatcher(graph)

    def get_relation_matcher(self, graph_name):
        graph = self.graph_service[graph_name]
        return RelationshipMatcher(graph)

    def find_relationship(self, n1, n2, graph_name=None):
        if graph_name is not None:
            relation_matcher = self.get_relation_matcher(graph_name)
        else:
            relation_matcher = self.default_relation_matcher
        node1, exists = self._get_node(n1)
        if not exists:
            logging.error("No node found for {entity}".format(entity=n1))
            return
        node2, exists = self._get_node(n2)
        if not exists:
            logging.error("No node found for {entity}".format(entity=n2))
            return
        match_output = relation_matcher.match((node1, node2))
        return type(match_output.first()).__name__

    def find_relation_tail(self,n1,relation, graph_name=None):
        print(f"entity {n1} relation: {relation}")
        if graph_name is not None:
            relation_matcher = self.get_relation_matcher(graph_name)
        else:
            relation_matcher = self.default_relation_matcher
        node1, exists = self._get_node(n1)
        if not exists:
            logging.error("No node found for {entity}".format(entity=n1))
            return
        relationship = relation_matcher.match([node1], r_type=self.transform_relation(relation))
        print(f"relationship: {relationship}")
        rel_str = str(relationship.first())
        pattern = re.compile('->\((.*)\)')
        answer = pattern.findall(rel_str)[0]
        return answer

