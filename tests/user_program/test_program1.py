from pyspark.sql import SparkSession

def func1():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("output.txt")
    
def func2():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("output.txt")

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.csv("output.txt")
df2 = spark.read.csv("output.txt")

df = df1.count()
df_new = df2.foo(1, 2)
