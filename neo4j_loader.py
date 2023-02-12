import pandas as pd
from neo4j import GraphDatabase
# from py2neo import Graph, Node, Relationship

def load_csv_into_db(file='processed.csv'):
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "your_password"))
    query = f'''
    LOAD CSV WITH HEADERS FROM "file://{file}" AS row
    MERGE (job1:{label1} {{jobId:"{jobId1}"}})
    MERGE (job2:{label2} {{JobId:"{jobId2}"}})
    MERGE (job1)-[r:IS_SIMILAR_TO {{weight:{weight}}}]-(job2)
    '''


def load_transaction_into_db(transaction, queries) -> None:
    for query in queries:
        transaction.run(query)

def batch_df_into_queries(df:pd.DataFrame, batch_size= 5000):
    driver =  GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "your_password"))
    batch = []
    for index, row in df.iterrows():
        jobId1 = row['JobId']
        jobId2 = row['SimilarJobId']
        label1 = row['BelongsTo']
        label2 = row['SimilarBelongsTo']
        member_score1 = row['MembershipScore']
        member_score2 = row['SimilarMembershipScore']
        weight = row['SimilarityScore']
        query = f'''
        MERGE (job1:{label1} {{JobId:"{jobId1}",membershipScore:{member_score1}}})
        MERGE (job2:{label2} {{JobId:"{jobId2}",membershipScore:{member_score2}}})
        MERGE (job1)-[r:IS_SIMILAR_TO {{weight:{weight}}}]-(job2)
        '''
        batch.append(query)
        if (index + 1) % batch_size == 0:
            with driver.session() as session:
                session.write_transaction(load_transaction_into_db,batch)
            print(f'Committed index {index}. \nCommitted {batch_size} transactions')
            batch = []
    if batch:
        with driver.session() as session:
                session.write_transaction(load_transaction_into_db,batch)














    # def load_df_into_db(df:pd.DataFrame, batch_size = 100) -> None:
    # graph = Graph("bolt://localhost:7687", auth=("neo4j", "your_password"))
    # batch = []
    # for index, row in df.iterrows():
    #     label1 = row['BelongsTo']
    #     label2 = row['SimilarBelongsTo']
    #     jobId1 = row['JobId']
    #     jobId2 = row['SimilarJobId']
    #     weight = row['SimilarityScore']
    #     query = f'''
    #     MERGE (job1:{label1} {{jobId:"{jobId1}"}})
    #     MERGE (job2:{label2} {{JobId:"{jobId2}"}})
    #     MERGE (job1)-[r:IS_SIMILAR_TO {{weight:{weight}}}]-(job2)
    #     '''

    #     #query = f'''
    #     #MERGE (job1:{label1} {{jobId:"{jobId1}"}})-[r:IS_SIMILAR_TO {{weight:{weight}}}]-(job2:{label2} {{JobId:"{jobId2}"}} )
    #     #MATCH (job1:{label1})
    #     #MERGE (job1:{label1} {{JobId:"{jobId1}"}})-[r:IS_SIMILAR_TO {{weight:{weight}}}]-(job2:{label2} {{JobId:"{jobId2}"}} )
    #     #'''
    #     print(index)
    #     tx=graph.begin()
    #     tx.update(query)
    #     batch.append(tx)
    #     if (index + 1) % batch_size == 0:
    #         for transaction in batch:
    #             graph.commit(transaction)
    #         batch = []
    #         print(f'Transaction # {index}')
    # if batch:
    #     graph.commit(tx)
    # print('Done')