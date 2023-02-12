import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable




class Api:

    def __init__(self) -> None:
        self.driver =  GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "your_password"))


    def close(self):
        self.driver.close()

    def project_graph(self):
        if not self.check_graph_exists():
            print(f'Projecting graph...')
            query = f'''
            CALL gds.graph.project(
                'myGraph',    
                ['c0', 'c1','c2', 'c3','c4', 'c5','c6', 'c7','c8', 'c9','c10', 'c11','c12', 'c13','c14', 'c15','c16', 'c17','c18', 'c19','c20', 'c21'],
                {{IS_SIMILAR_TO:{{orientation:"UNDIRECTED", properties:"weight"}}}}        
            )
            YIELD
            graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipCount AS rels
            '''
            with self.driver.session() as session:
                session.run(query)
            print('Graph projected!')
        else:
            print(f'Graph already exists')

    def check_graph_exists(self,graph='myGraph'):
        query = f'''
            CALL gds.graph.exists("{graph}")
                YIELD graphName, exists
            RETURN graphName, exists
        '''
        with self.driver.session() as session:
            result = session.run(query)
            exists = result.values()[0][1]
            return exists
            
        

    @staticmethod
    def _get_top_nodes(transaction, n: int, cluster: str):

        query = f'''
        MATCH (node:{cluster})
        RETURN node
        ORDER BY node.membershipScore DESC
        LIMIT {n}
        '''
        results = transaction.run(query)
        try:
            #return [{"node":record[]}]
            return [{'JobId':record['node']['JobId'],'membershipScore':record['node']['membershipScore']} for record in results]
        except ServiceUnavailable as exception:
            logging.error(f'{query} raised an error:\n {exception}')
            raise

        
    def get_top_nodes(self, n, cluster):
        with self.driver.session() as session:
            results = session.execute_read(self._get_top_nodes,n,cluster)
            return results

    
# Shortest paths

    #By edge weight

    @staticmethod
    def _get_shortest_path_by_weight(transaction, jobId1: str, jobId2: str):
        query = f'''
        MATCH (source), (target)
        WHERE source.JobId="{jobId1}" AND target.JobId="{jobId2}"
        CALL gds.shortestPath.dijkstra.stream('myGraph', {{
            sourceNode: source,
            targetNode: target,
            relationshipWeightProperty: 'weight'
        }})
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
        RETURN
            index,
            gds.util.asNode(sourceNode).name AS sourceNodeName,
            gds.util.asNode(targetNode).name AS targetNodeName,
            totalCost,
            [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
            costs,
            nodes(path)
        ORDER BY index
        '''
        results = transaction.run(query)
        try:
            #[node['properties'] for node in record['path']]
            #return [{'costs':record['costs'],'path':[node['JobId'] for node in record['path']]} for record in results ]
            return [ [{'JobId':node['JobId'],'membershipScore':node['membershipScore']} for node in record[f'nodes(path)']] for record in results]
        except ServiceUnavailable as exception:
            logging.error(f'{query} raised an error:\n {exception}')
            raise

        
    def get_shortest_path_by_weight(self, jobId1: str, jobId2: str):
        with self.driver.session() as session:
            results = session.execute_read(self._get_shortest_path_by_weight,jobId1,jobId2)
            return results


    # By number of nodes

    @staticmethod
    def _get_shortest_path_by_numnodes(transaction, jobId1: str, jobId2: str):

        # query = f'''
        # MATCH (source), (target)
        # WHERE source.JobId="{jobId1}" AND target.JobId="{jobId2}"
        # WITH id(source) AS source, [id(target)] AS targetNodes
        # CALL gds.bfs.stream('myGraph', {{
        #     sourceNode: source,
        #     targetNodes: targetNodes
        # }})
        # YIELD path
        # RETURN path, nodes(path)
        # '''

        query=f'''
        MATCH (source), (target)
        WHERE source.JobId="{jobId1}" AND target.JobId="{jobId2}"
        MATCH path = shortestPath((source)-[*]-(target))
        RETURN path,nodes(path)
        '''
        results = transaction.run(query)
        try:
            return [ [{'JobId':node['JobId'],'membershipScore':node['membershipScore']} for node in record[f'nodes(path)']] for record in results]
        except ServiceUnavailable as exception:
            logging.error(f'{query} raised an error:\n {exception}')
            raise

        
    def get_shortest_path_by_numnodes(self, jobId1: str, jobId2: str):
        with self.driver.session() as session:
            results = session.execute_read(self._get_shortest_path_by_numnodes,jobId1,jobId2)
            return results