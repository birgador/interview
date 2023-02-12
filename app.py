from flask import Flask, jsonify, request
from models.neo4j_logic import Api


app = Flask(__name__)

@app.route("/")
def hello_world():
    return "Hello, World!"

@app.route("/api/top_n", methods=['GET'])
def get_top_n():
    n =int(request.args['n'])
    cluster = request.args['cluster']
    nodes = Api().get_top_nodes(n,cluster)
    return jsonify(nodes)

@app.route("/api/find_shortest/path_weight")
def get_shortest_path_weight():

    jobId1 =request.args['JobId1']
    jobId2 = request.args['JobId2']
    nodes = Api().get_shortest_path_by_weight(jobId1,jobId2)

    return jsonify(nodes)

@app.route("/api/find_shortest/numnodes")
def get_shortest_path_numnodes():

    jobId1 =request.args['JobId1']
    jobId2 = request.args['JobId2']
    nodes = Api().get_shortest_path_by_numnodes(jobId1,jobId2)
    
    return jsonify(nodes)

Api().project_graph()
if __name__=='__main__':
    app.run()