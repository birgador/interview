import time
import pandas as pd
from processing_and_loading.data_processing import load_data, assign_cluster, calculate_self_similarity_matrix, get_membership_score, get_similarities_from_matrix, get_relation_pairs
from processing_and_loading.neo4j_loader import load_df_into_db_batch, check_db_online, check_data_in_db
from db_logic.neo4j_logic import Api

class DataProcessor:

    def __init__(self) -> None:
        pass

    def process_data(self):
        # Check db is online before proceeding. I couldn't find a way to prevent queries from retrying, so the retry here seems to not make sense 
        # It makes sense for the first time this is run
        db_online = False
        print('Checking if database is online...')
        time.sleep(30)
        while db_online == False:
            db_online = check_db_online()

            # Wait 10s if not online and retry
            if db_online == False:
                print('Retrying in 10s')
                time.sleep(10)

        # Check databased has data from script (THIS IS NOT FOR PRODUCTION, JUST FOR INTERVIEW)
        data_in_db = check_data_in_db()
        # No records on the db
        if data_in_db == 0:          

            print('Processing data...')
            url = 'http://dropbox.jobtome.com/data/samples/job_graph_matrix.csv'

            # load data and define numerical columns
            df = load_data(url)
            cluster_cols = [f'c{col_num}' for col_num in range(22)]

            # identify the cluster the job belongs to and create the membershipscore column
            df['BelongsTo'] = assign_cluster(df[cluster_cols])
            df["MembershipScore"] = get_membership_score(df[cluster_cols])

            # Calculate similarities
            similarity_matrix = calculate_self_similarity_matrix(df[cluster_cols])
            similarities = get_similarities_from_matrix(similarity_matrix)

            # Identify relationships
            relation_pairs = get_relation_pairs(similarity_matrix)
            jobs = relation_pairs[:,0]
            similar_jobs = relation_pairs[:,1]
            df_jobs = df.iloc[jobs]
            df_similar_jobs = df.iloc[similar_jobs]

            # Obtain new dataframe
            processed_df = pd.DataFrame(
                {
                    'JobId':df_jobs['JobId'].values,
                    'SimilarJobId':df_similar_jobs['JobId'].values,
                    'BelongsTo':df_jobs['BelongsTo'].values,
                    'MembershipScore':df_jobs['MembershipScore'].values,
                    'SimilarMembershipScore':df_similar_jobs['MembershipScore'].values,
                    'SimilarBelongsTo':df_similar_jobs['BelongsTo'].values,
                    'SimilarityScore':similarities
                }
            )
            # Save dataframe to csv and load into neo4j
            #processed_df.to_csv('processed.csv',header=True,index=None)
            print(f'Processing done!\n Loading data into database:')

            # Load dataframe into db in batches (default 5000)
            load_df_into_db_batch(processed_df)
            print('Data loaded!')
            del processed_df    # Free up space
            

        else:
            print('Data from this script already in db!')
        Api().project_graph()


if __name__ == '__main__':
    DataProcessor().process_data()