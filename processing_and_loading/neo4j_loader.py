import pandas as pd
from neo4j import GraphDatabase
# from py2neo import Graph, Node, Relationship


# Execute transaction
def run_transaction(transaction, queries) -> None:
    for query in queries:
        transaction.run(query)

# Load data into db
def load_df_into_db_batch(df:pd.DataFrame, batch_size= 5000):

    # Initialise driver
    driver =  GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "your_password"))

    batch = []

    # iterate through dataframe to generate the queries
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

        # Append query to the batch and load into db
        batch.append(query)
        if (index + 1) % batch_size == 0:
            with driver.session() as session:
                session.write_transaction(run_transaction,batch)
            print(f'Committed index {index}. \nCommitted {batch_size} transactions')
            batch = []
            
    # Load into db leftover queries
    if batch:
        with driver.session() as session:
                session.write_transaction(run_transaction,batch)
