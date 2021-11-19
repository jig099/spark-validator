'''
from math import radians, cos, sin, asin, sqrt
def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """
    Calculate the great circle distance between two points on the 
    earth (specified in decimal degrees), returns the distance in
    meters.
    All arguments must be of equal length.
    :param lon1: longitude of first place
    :param lat1: latitude of first place
    :param lon2: longitude of second place
    :param lat2: latitude of second place
    :return: distance in meters between the two sets of coordinates
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers
    return c * r

flow:
Black box design
1. user will provide input file, sample paramters
2. then spark validator will do the following
    a. parse user's code
        i) find the dataset in user's code
        ii) sample the dataset via the sample_params
        iii) create sample node and inject node into the ast tree
        iv) create two versions of file
            * one is the original file with only sampling code injected
            * one is the generated file with both sampling code and rewritten workflow
        a
    b. sample data input
        i) sampling the input data file with the specified sampler
        ii) output the sampled data 
   
def spark_validate(input_file, sample_params, user_program):
    """
    test_function does blah blah blah.

    :param p1: describe about parameter p1
    :param p2: describe about parameter p2
    :param p3: describe about parameter p3
    :return: describe what it returns

-------------------------------------------------------------
library design
will provide a list of functions for users to use
1. sample functions (different types, size etc)
2. convert common spark functions
'''

import pandas
import cProfile
import re

### sample APIs###
# 1. random_sampling()
# 2. stratified_sampling()
# 3. systematic_sampling()
# 4. clustered_sampling()


def sample(input_dataset, with_replacement, fraction, seed):
    """
    https://sparkbyexamples.com/pyspark/pyspark-sampling-example/
    sample function will take in either a dataframe or RDD 
    and return sampled data back to the user

    :param input_dataset: user input dataset could be either a dataframe or RDD
    :param with_replacement: will return a random sample with duplicates
    :param seed: reproduce the same samples
    :return: return sampled dataset
    """ 

def stratified_sampling():
    return 


### Translation APIs ###
## basic operations on dataframes ##
# 1. join()
# 2. reshape()
# 3. min() / max()
# 4. grouping() / aggregation()
# 5. map()
# 6. reduce()
# 7. count()
# 8. sum()
# 9. average()
# 10. rename()
# 11. replaceNull()
# def vjoin():

### Profiler APIs ###
##https://docs.python.org/3/library/profile.html##
##pyspark profiler source code: https://spark.apache.org/docs/2.3.1/api/python/_modules/pyspark/profiler.html##
## run the cprofile and analyze the result
def profiler(user_file):
    cProfile.run('re.compile("foo|bar")')