try:
    from flask import Flask, request, Response
    import boto3
    from mongo_extractor import mongo_extractor
    from celery import Celery
    import pymongo
    import json
    import sys
    from flask_cors import CORS, cross_origin
    from logging.config import dictConfig
    import logging
    from template_explorer import template_explorer
    from kg_explorer import kg_explorer
    import helper
except Exception as e:
    print("Error  :{} ".format(e))

logging.basicConfig(level=logging.INFO)
root = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
root.addHandler(handler)
app = Flask(__name__)
scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
host_name = "neo4j_container"
port = 7687
url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
user = "neo4j"
password = "test"
kg_explorer = kg_explorer.KGExplorer(url, user, password)
template_explorer = template_explorer.ElasticTemplateExplorer(kg_explorer, "http://elastic_container:9200")
_ = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
mongo_app = mongo_extractor.MongoExtractor("mongodb://mongodb_container:27017/", "ingestion_db", "ingestion_logs")
celery_app = Celery('celery_app',
                    broker='amqp://admin:mypass@rabbit:5672',
                    backend='mongodb://mongodb_container:27017/mydb')


@app.route('/')
@cross_origin()
def health_check():
    return "Hello from KG app"


@app.route('/create_graph', methods=["POST"])
def create_graph():
    data = request.get_json(force=True)
    if 'graph_name' not in data:
        return Response("{'message': 'graph name not found in request'}", status=400, mimetype='application/json')
    graph_name = data['graph_name']
    resp = {"message": "{graph_name} created successfully".format(graph_name=graph_name)}
    return Response(json.dumps(resp), status=200, mimetype='application/json')


@app.route('/extract_relations', methods=["POST"])
@cross_origin()
def create_relations():
    data = request.get_json(force=True)
    result = celery_app.send_task('celery_task_app.tasks.RelationExtraction', args=[data])
    response = {
        "task_id": result.id
    }
    app.logger.info(result.backend)
    return json.dumps(response['task_id'])


@app.route('/get_ingestion_status')
@cross_origin()
def get_ingestion_status():
    task_id = request.args.get("task_id")
    if task_id is None:
        Response(json.dumps({'message': 'task id not present in the input'}), status=400, mimetype='application/json')
    task = celery_app.AsyncResult(task_id, app=celery_app)
    if task.ready():
        ingestion_id = task.result['ingestion_id']
        mongo_app.update_doc(task_id, ingestion_id)
        response = {
            "status": "DONE"
        }
    else:
        response = {
            "status": "IN_PROGRESS"
        }
    return response['status']


@app.route('/get_sample_relations')
@cross_origin()
def get_sample_relations():
    task_id = request.args.get("task_id")
    if task_id is None:
        Response(json.dumps({'message': 'task id not present in the input'}), status=400, mimetype='application/json')
    output = mongo_app.get_sample_relations(task_id=task_id)
    if 'task_id' not in output:
        return Response(json.dumps({'message': 'no such task found'}), status=400, mimetype='application/json')
    resp = {'relations': output['relations']}
    return Response(json.dumps(resp['relations']), status=200, mimetype='application/json')


@app.route('/add_template', methods=["POST"])
@cross_origin()
def add_template():
    data = request.get_json(force=True)
    template_explorer.ingest_template(relation=data['relation'], template=data['template'],
                                      groups=helper.create_groups(data['entity_group']))
    return {"message": "template ingested successfully"}


@app.route('/search_template', methods=["POST"])
@cross_origin()
def search_template():
    data = request.get_json(force=True)
    return template_explorer.search_template(query=data['question'])


@app.route('/get_relation_list')
@cross_origin()
def get_relation_list():
    return Response(json.dumps(template_explorer.get_relation_list()), status=200, mimetype='application/json')


@app.route('/get_template')
@cross_origin()
def get_templates():
    relation = request.args.get("relation")
    templates = template_explorer.get_templates(relation=relation)
    return Response(json.dumps(templates), status=200, mimetype='application/json')