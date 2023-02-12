import pandas as pd
import numpy as np
import sklearn.metrics as metrics

def load_data(data)-> pd.DataFrame:
    return pd.read_csv(data)

def assign_cluster(df:pd.DataFrame) -> pd.Series:
    return df.idxmax(axis=1)

def get_membership_score(df:pd.DataFrame) -> pd.Series:
    return df.max(axis=1)

def calculate_self_similarity_matrix(df:pd.DataFrame):
    return metrics.pairwise.cosine_similarity(df,df)

def get_similarities_from_matrix(matrix,weight_threshold = 0.99):
    triangular_matrix = erase_diags(matrix)
    similarities = triangular_matrix[triangular_matrix >= weight_threshold]   # array of scalars
    return similarities

def erase_diags(array):
  copy_array = np.copy(array) # fill_diagonal is a destructive method
  np.fill_diagonal(copy_array,0)
  return np.triu(copy_array)

def get_relation_pairs(array, weight_threshold = 0.99):
  return np.argwhere(erase_diags(array) >= weight_threshold)