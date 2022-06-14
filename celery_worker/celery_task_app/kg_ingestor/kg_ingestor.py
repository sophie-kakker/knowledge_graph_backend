from py2neo import Graph, Node, Relationship, NodeMatcher, RelationshipMatcher, GraphService
import logging


class KGIngestor:
    def __init__(self, url, user, password):
        self.graph_service = GraphService(url, auth=(user, password))
        logging.info("graph service initiated successfully")
        self.default_graph = self.graph_service.default_graph
        self.default_node_matcher = NodeMatcher(self.default_graph)
        self.default_relation_matcher = RelationshipMatcher(self.default_graph)
        self.create_unique_constraint()

    def transform_relation(self, relation):
        return relation.replace(" ", "_")

    def create_unique_constraint(self, graph_name=None):
        if graph_name is None:
            self.default_graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (x:ENTITY) ASSERT x.name IS UNIQUE")
        else:
            self.graph_service[graph_name].run("CREATE CONSTRAINT IF NOT EXISTS ON (x:ENTITY) ASSERT x.name IS UNIQUE")

    def create_relationship(self, node1, relation, node2, graph_name=None):
        try:
            if graph_name == None:
                graph = self.graph_service.default_graph
            else:
                if graph_name not in self.graph_service:
                    logging.error("Graph: {graph_name} does not exist."
                                  " Please create the graph first.".format(graph_name=graph_name))
                    return False
                else:
                    graph = self.default_graph
            tx = graph.begin()
            node1 = self._get_or_create_node(tx, node1)
            node2 = self._get_or_create_node(tx, node2)
            relation = self.transform_relation(relation)
            n1n2 = Relationship(node1, relation, node2)
            tx.create(n1n2)
            tx.commit()
        except Exception as e:
            logging.error(f"failed to ingest relation {node1} -{relation}-> {node2} error: {e}")

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

    def _get_or_create_node(self, tx, node_name, node_label="ENTITY", graph_name=None):
        if graph_name is not None:
            node_matcher = self.get_node_matcher(graph_name)
        else:
            node_matcher = self.default_node_matcher
        node_match = node_matcher.match(node_label, name=node_name)
        if node_match.exists():
            return node_match.first()
        else:
            nd = Node(node_label, name=node_name)
            tx.create(nd)
            return nd

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

    def find_relation_tail(self, n1, relation, graph_name=None):
        if graph_name is not None:
            relation_matcher = self.get_relation_matcher(graph_name)
        else:
            relation_matcher = self.default_relation_matcher
        node1, exists = self._get_node(n1)
        if not exists:
            logging.error("No node found for {entity}".format(entity=n1))
            return
        relationship = list(relation_matcher.match([node1], r_type=self.transform_relation(relation)))
        return relationship[0]
