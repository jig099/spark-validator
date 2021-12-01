import pyspark


# create spark session and read in the data
spark = pyspark.sql.SparkSession.builder.appName('demo').getOrCreate()
df = spark.read.option("header","true").csv('data/walmart_stock.csv')

# select useful columns - Open, High, Low, Close
df = df.select(df['Open'].cast('float'),df['High'].cast('float'),df['Low'].cast('float'),df['Close'].cast('float'))

# count number of data entries in the file
entryCount = df.count()
print("total number of entries: ",entryCount)

# find the max stock price
HighPriceDf = df.select(df['High'])
maxPriceDf = HighPriceDf.groupBy()
maxPriceDf = maxPriceDf.max()
maxPriceDf.show()

# find max price fluctuation and final price change in a day
df = df.rdd.map(lambda x : (x[1] - x[2],x[3] - x[0])).toDF(["max_price_fluctuation","final_price_change"])
df.show()

# find average price fluctuation
fluctuationDf = df.select(df["max_price_fluctuation"])
fluctuationDf = fluctuationDf.groupBy()
fluctuationDf = fluctuationDf.avg()
fluctuationDf.show()