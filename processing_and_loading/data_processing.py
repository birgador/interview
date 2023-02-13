import pandas as pd
import numpy as np
import sklearn.metrics as metrics

# Loads csv data from source
def load_data(data)-> pd.DataFrame:
    return pd.read_csv(data)

# Determines the label of the column with highest value (the cluster of the job)
def assign_cluster(df:pd.DataFrame) -> pd.Series:
    return df.idxmax(axis=1)

# Gets de membership score (max value from all clusters)
def get_membership_score(df:pd.DataFrame) -> pd.Series:
    return df.max(axis=1)

# Calculates similarity matrix of every job against the others 
def calculate_self_similarity_matrix(df:pd.DataFrame):
    return metrics.pairwise.cosine_similarity(df,df)

# Gets the similarity scores with the threshold applied
def get_similarities_from_matrix(matrix,weight_threshold = 0.99):

    # Generates a triangular matrix to eliminate duplicates
    triangular_matrix = erase_diags(matrix)
    similarities = triangular_matrix[triangular_matrix >= weight_threshold]   # array of scalars
    return similarities

# Erases de diagonal of the matrix as its elements represent the similarity of a job against itself
def erase_diags(array):
  copy_array = np.copy(array) # fill_diagonal is a destructive method
  np.fill_diagonal(copy_array,0)
  return np.triu(copy_array)

# Gets the indexes of the similarity scores from the similarities matrix 
def get_relation_pairs(array, weight_threshold = 0.99):
  return np.argwhere(erase_diags(array) >= weight_threshold)