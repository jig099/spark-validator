from pyspark.sql import SparkSession

def func1():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("output.txt")
    
def func2():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("output.txt")
    