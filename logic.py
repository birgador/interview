class Cluster:
    pass

class Node:

    '''
    A node has a parent cluster, a list of links
    '''
    def __init__(self,id: str, edges = []) -> None:
        self.id = id
        pass
