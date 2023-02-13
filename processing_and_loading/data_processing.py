import pandas as pd
import numpy as np
import sklearn.metrics as metrics

# Loads csv data from source
def load_data(data)-> pd.DataFrame:
    """Loads the csv data to a pandas Dataframe

    Parameters
    ----------
    data : str
        The file location of the csv file
    
    Returns
    -------
    DataFrame
        a Pandas DataFrame containing the data from the csv file
    """
    return pd.read_csv(data)

# Determines the label of the column with highest value (the cluster of the job)
def assign_cluster(df:pd.DataFrame) -> pd.Series:
    """Provides a Series with the column name of the highest value in each row

    Parameters
    ----------
    df : DataFrame
        A pandas DataFrame
    
    Returns
    -------
    Series
        a Pandas Series containing the labels of the highest column for each numeric row
    """
    return df.idxmax(axis=1)

# Gets de membership score (max value from all clusters)
def get_membership_score(df:pd.DataFrame) -> pd.Series:
    """Provides a Series with the max value of each row

    Parameters
    ----------
    df : DataFrame
        A pandas DataFrame
    
    Returns
    -------
    Series
        a Pandas Series containing the value of the highest column for each numeric row
    """
    return df.max(axis=1)

# Calculates similarity matrix of every job against the others 
def calculate_self_similarity_matrix(df:pd.DataFrame):
    """Calculates the similarity of a numeric DataFrame with itself

    Parameters
    ----------
    df : DataFrame
        A pandas DataFrame
    
    Returns
    -------
    numpy.ndarray
        a numpy matrix array containing the cosine similarities
    """
    return metrics.pairwise.cosine_similarity(df,df)

# Gets the similarity scores with the threshold applied
def get_similarities_from_matrix(matrix,weight_threshold = 0.99):
    """Calculates the similarity of a numeric DataFrame with itself

    Parameters
    ----------
    matrix : numpy.ndarray
        A numpy matrix array containing the cosine similarities
    weight_threshold : int
        An integer representing the cutoff threshold to keep the similarities
   
    
    Returns
    -------
    array
        a numpy 1D array containing the values of the cosine similarities
    """

    # Generates a triangular matrix to eliminate duplicates
    triangular_matrix = erase_diags(matrix)
    similarities = triangular_matrix[triangular_matrix >= weight_threshold]   # array of scalars
    return similarities

# Erases de diagonal of the matrix as its elements represent the similarity of a job against itself
def erase_diags(array):
    """Calculates the similarity of a numeric DataFrame with itself

    Parameters
    ----------
    array : numpy.ndarray
        A numpy matrix array to delete the diagonal
    
    Returns
    -------
    array
        a numpy matrix array with 0s on the diagonal
    """
    copy_array = np.copy(array) # fill_diagonal is a destructive method
    np.fill_diagonal(copy_array,0)
    return np.triu(copy_array)

# Gets the indexes of the similarity scores from the similarities matrix 
def get_relation_pairs(array, weight_threshold = 0.99):
    """Calculates the similarity of a numeric DataFrame with itself

    Parameters
    ----------
    matrix : numpy.ndarray
        A numpy matrix array containing the cosine similarities
    weight_threshold : int
        An integer representing the cutoff threshold to keep the similarities
    
    Returns
    -------
    array
        a numpy 1D array containing the indexes of the non-zero values of the array
    """
    
    return np.argwhere(erase_diags(array) >= weight_threshold)