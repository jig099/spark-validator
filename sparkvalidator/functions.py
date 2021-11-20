from pyspark.sql.functions import col, asc, desc
from functools import reduce
from itertools import groupby
from operator import (itemgetter, add)

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
# 12. sort()
# 13. filter()


def join(left, right):
    '''
    Parameters: 
        left: DataFrame to be joined
        right: Right side of the join
    Returns: 
        ps_result: result of join in pyspark dataframe
    '''
    # run pyspark join on the datasets
    ps_result = left.join(right)

    # run pandas join on the datasets
    left_pd, right_pd = left.toPandas(), right.toPandas()
    pd_result = left_pd.join(right_pd)

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "join")

    return ps_result


def min(df, col):
    '''
    Parameters: 
        df: pyspark dataframe
        col: column to take min
    Return:
        ps_result: pyspark dataframe after taken min
    '''
    # run min on pyspark dataframe
    ps_result = df.groupBy().min(col)

    # run min on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.groupby(col).min()

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "average")

    return ps_result


def max(df, col):
    '''
    Parameters: 
        df: pyspark dataframe
        col: column to take max
    Return:
        ps_result: pyspark dataframe after taken max
    '''
    # run max on pyspark dataframe
    ps_result = df.groupBy().max(col)

    # run max on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.groupby(col).max()

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "average")

    return ps_result


def grouping(df, group_name):
    '''
    Parameters:
        df: user input pyspark dataframe
        group_name: colume name to perform grouping
    Return: 
        ps_result: result after grouping pyspark dataframe
    '''
    # run grouping function on pyspark dataframe
    ps_result = df.groupBy(group_name)

    # run grounping function on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.groupby(group_name)

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "grouping")

    return ps_result


def map(df, func, schema):
    '''
    Parameters:
        df: user input pyspark dataframe
        func: lambda function to be applied to each row of df 
        schema: column names for the result dataframe 
    Return: 
        ps_result: result after grouping pyspark dataframe
    '''
    ps_result = df.rdd.map(lambda x: func(x)).toDF(schema)
    pd_result = df.toPandas().apply(lambda x: func(x), axis=1)
    pd_result.rename(columns=schema, inplace=True)
    compare(ps_result, pd_result, 'map')

    return ps_result

# https://gist.github.com/chemacortes/84d6441518b77b232ecb


def reduceByKey(df, func):
    '''
    Parameters:
        df: user input pyspark dataframe
        func: lambda function to be applied to key value pairs in the dataframe
    Return: 
        ps_result: result after grouping pyspark dataframe
    '''
    rdd = df.rdd
    ps_result = rdd.reduceByKey(func).toDF()
    pd_df = df.toPandas()
    pd_result = ((k, reduce(func, (x[1] for x in xs)))
                 for (k, xs) in groupby(sorted(pd_df, key=itemgetter(0)), itemgetter(0))
                 )
    compare(ps_result, pd_result, 'reduceByKey')
    return ps_result


def count(df):
    '''
    Parameters:
        df: user input pyspark dataframe
    Return: 
        ps_result: result after counting pyspark dataframe
    '''
    ps_result = df.count()
    pd_result = len(df.toPandas())
    compare(ps_result, pd_result, 'count')
    return ps_result


def sum(df):
    '''
    Parameters:
        df: user input pyspark dataframe
    Return: 
        ps_result: result after summing pyspark dataframe
    '''
    # run sort function on pyspark dataframe
    ps_result = df.groupBy().sum()

    # run sort function on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.sum(axis=0, skipna=True)

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "sum")

    return


def average(df, col):
    '''
    Parameters: 
        df: pyspark dataframe
        col: column to be averaged
    Return:
        ps_result: pyspark dataframe after average
    '''
    # run average on pyspark dataframe
    ps_result = df.groupBy().avg(col)

    # run average on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.groupby(col).mean()

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "average")

    return ps_result


def renameColumn(df, existing, new):
    '''
    Parameters: 
         df: pyspark dataframe
         existing: existing name of the column to be renamed
         new: new name of the column to be renamed
    Return:
         ps_result: pyspark dataframe after renameColumn
    '''
    # run renameColumn on pyspark dataframe
    ps_result = df.withColumnRenamed(existing, new)

    # run renameColumn on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.rename(columns=dict(existing))

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "renameColumn")

    return ps_result


def replaceNull(df, value=0):
    '''
    Parameters: 
         df: pyspark dataframe
         value: value that will replace Null
    Return:
         ps_result: pyspark dataframe after replaceNull
    '''

    # run replaceNull function on pyspark dataframe
    ps_result = df.na.fill(value)

    # run replaceNull function on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.fillna(value)

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "replaceNull")

    return ps_result


def sort(df, col_name, asc=True):
    '''
    Parameters:
        df: user input pyspark dataframe
        col_name: colume name to perform sort
        asc: indicator of sort in ascending order, default to true
    Return: 
        ps_result: result after sorting pyspark dataframe
    '''
    # run sort function on pyspark dataframe
    if asc:
        ps_result = df.sort(col(col_name).asc())
    else:
        ps_result = df.sort(col(col_name).desc())

    # run sort function on pandas dataframe
    pd_df = df.toPandas()
    pd_result = pd_df.sort_values(col_name, asc)

    # compare results from pyspark and pandas
    compare(ps_result, pd_result, "sort")

    return ps_result


def filter(df, condition: str):
    '''
    Parameters:
        df: user input pyspark dataframe
        condition: a query condition to specify if a row should be kept
    Return: 
        ps_result: result after sorting pyspark dataframe
    '''
    ps_result = df.filter(condition)
    pd_result = df.toPandas().query(condition)
    compare(ps_result, pd_result, 'filter')
    return ps_result


## utility functions ##
def compare(ps_result, pd_result, f_name):
    if isinstance(pd_result, pd.DataFrame):
        ps_m_result = ps_result.toPandas()
        comparison = pd_result.equals(ps_m_result)
    else:
        comparison = (ps_result == pd_result)
    # raise assertion error if not equal
    if not comparison:
        print(ps_result)
        print(pd_result)
        raise AssertionError("Potential error in %s" % f_name)
