from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable




class Api:
    """
    A class to represent the API

    ...
    Methods
    -------
    close()
        Closes the driver

    project_graph()
        Projects a graph to the Graph Catalog to allow running functions from gds

    check_graph_exists(graph="myGraoh")
        Checks if a graph with the name in graph exists in the Graph Catalog

    _get_top_nodes(transaction: Transaction, n: int, cluster: str) -> list
        Runs a query to the database to retrieve the top N nodes belonging to the cluster cluster
        and returns a list of dictionaries containing the job ID and the membership score of the node
    
    get_top_nodes(n, cluster) -> list
        Method called when a request is made on the endpoint. Creates the database session to run the queries executed by 
        _get_top_nodes

    _get_shortest_path_by_weight(transaction: Transaction, jobId1: str, jobId2: str) -> list
        Runs a query to the database to retrieve the shortest path between 2 nodes based on the weight of the edges
        and returns a list of dictionaries containing the job ID and the membership score of the node

    get_shortest_path_by_weight(jobId1: str, jobId2: str)
        Method called when a request is made on the endpoint. Creates the database session to run the queries executed by 
        _get_shortest_path_by_weight

    _get_shortest_path_by_num_nodes(transaction: Transaction, jobId1: str, jobId2: str) -> list
        Runs a query to the database to retrieve the shortest path between 2 nodes based on the number of nodes
        and returns a list of dictionaries containing the job ID and the membership score of the node

    get_shortest_path_by_num_nodes( jobId1: str, jobId2: str)
        Method called when a request is made on the endpoint. Creates the database session to run the queries executed by 
        _get_shortest_path_by_num_nodes
    """

    def __init__(self) -> None:
        self.driver =  GraphDatabase.driver("bolt://database:7687", auth=("neo4j", "your_password"))


    def close(self):
        """Closes the driver

        Currently unused as we open and close the driver with "with"

        """
        self.driver.close()

    
    # We need to project the graph to the Graph Catalog in order to run functions from gds

    def project_graph(self):

        """Projects the Graph to the Graph Catalog

        To run functions from the Data Science plugin, we need to project the graphs we want to work on
        to the Graph Catalog of the Database

        """

        # We check if the graph myGraph exists in Graph Catalog, if not we run the query to create it
        # query exports all available labels (the clusters), their relationship and the properties of the relationship. 
        # Algorithms take direction into account, therefore we use UNDIRECTED

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

        # Check if graph exists in Graph Catalog

    def check_graph_exists(self,graph='myGraph'):
        """Checks if a graph exists in the Graph Catalog

        Graphs are dropped when the session is closed, so we need to check if a graph exist

        Parameters
        ----------
        graph : str, optional
            The name of the graph in the Graph Catalog

        Returns
        -------
            bool
                True if the graph exists
        """

        query = f'''
            CALL gds.graph.exists("{graph}")
                YIELD graphName, exists
            RETURN graphName, exists
        '''
        with self.driver.session() as session:
            result = session.run(query)
            exists = result.values()[0][1]  # Returns a list with a list with the graphName and exists
            return exists
            
        
        # All static methods execute the query for the session.

    @staticmethod
    def _get_top_nodes(transaction, n: int, cluster: str):
        """Executes the query to get the top N nodes

        Parameters
        ----------
        n : int
            The top N nodes to retrieve
        cluster : str
            The cluster name

        Returns
        -------
        list
            a list of dictionaries containing the job IDs and and the membership scores
        """

        query = f'''
        MATCH (node:{cluster})
        RETURN node
        ORDER BY node.membershipScore DESC
        LIMIT {n}
        '''
        try:
            results = transaction.run(query)
            return [{'JobId':record['node']['JobId'],'membershipScore':record['node']['membershipScore']} for record in results]
        except ServiceUnavailable as exception:
            print(f'{query} raised an error:\n {exception}')
            raise

        
    def get_top_nodes(self, n, cluster):
        """Creates a session on demand when a request is received by the endpoint to retrieve the
            top N nodes

        Parameters
        ----------
        n : int
            The top N nodes to retrieve
        cluster : str
            The cluster name

        Returns
        -------
        list
            a list of dictionaries containing the job IDs and and the membership scores
        """
        with self.driver.session() as session:
            results = session.execute_read(self._get_top_nodes,n,cluster)
            return results

    
# Shortest paths

    #By edge weight

    @staticmethod
    def _get_shortest_path_by_weight(transaction, jobId1: str, jobId2: str):
        """Executes the query to get the path between two nodes based on edge weight

        Parameters
        ----------
        jobId1 : str
            Job ID of one of the nodes
        jobId1 : str
            Job ID of the other node

        Returns
        -------
        list
            a list of dictionaries containing the job IDs and and the membership scores
        """

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
        
        try:
            results = transaction.run(query)
            return [ [{'JobId':node['JobId'],'membershipScore':node['membershipScore']} for node in record[f'nodes(path)']] for record in results]
        except ServiceUnavailable as exception:
            print.error(f'{query} raised an error:\n {exception}')
            raise

        
    def get_shortest_path_by_weight(self, jobId1: str, jobId2: str):
        """Creates a session on demand when a request is received by the endpoint to retrieve the
            list of nodes between two nodes by weight of the edge

        Parameters
        ----------
        jobId1 : str
            Job ID of one of the nodes
        jobId1 : str
            Job ID of the other node

        Returns
        -------
        list
            a list of dictionaries containing the job IDs and and the membership scores
        """
        with self.driver.session() as session:
            results = session.execute_read(self._get_shortest_path_by_weight,jobId1,jobId2)
            return results


    # By number of nodes

    @staticmethod
    def _get_shortest_path_by_num_nodes(transaction, jobId1: str, jobId2: str):
        """Executes the query to get the path between two nodes based on number of nodes.

        The commented out query also works, as it uses Breadth first to find the shortest path
        However, the shortestPath query is simpler

        Parameters
        ----------
        jobId1 : str
            Job ID of one of the nodes
        jobId1 : str
            Job ID of the other node

        Returns
        -------
        list
            a list of dictionaries containing the job IDs and and the membership scores
        """

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
        
        try:
            results = transaction.run(query)
            return [ [{'JobId':node['JobId'],'membershipScore':node['membershipScore']} for node in record[f'nodes(path)']] for record in results]
        except ServiceUnavailable as exception:
            print(f'{query} raised an error:\n {exception}')
            raise

        
    def get_shortest_path_by_num_nodes(self, jobId1: str, jobId2: str):
        """Creates a session on demand when a request is received by the endpoint to retrieve the
            list of nodes between two nodes by the number of nodes in the path

        Parameters
        ----------
        jobId1 : str
            Job ID of one of the nodes
        jobId1 : str
            Job ID of the other node

        Returns
        -------
        list
            a list of dictionaries containing the job IDs and the membership scores
        """
        with self.driver.session() as session:
            results = session.execute_read(self._get_shortest_path_by_num_nodes,jobId1,jobId2)
            return results