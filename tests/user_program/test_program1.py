program = 'import pyspark
from pyspark.sql import Row
from pyspark.sql import SQLContext

sqlContext = SQLContext()
# Load a text file and convert each line to a Row.
lines = sqlContext.textFile("examples/src/main/resources/people.txt")
