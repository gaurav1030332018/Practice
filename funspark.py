from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Bad record Handling").getOrCreate()

df=spark.read.format("CSV").option("header","true").option("inferschema","true").load("C:\Users\Gaurav bhoi\Downloads\emp_without_header.csv")

#define
def convToZero(x):
	if x>=0:
		return x
	else:
		return 0

#register
convToZero=udf(convToZero,IntegerType())

#apply

df1=df.withColumn("temp",convToZero(df['temp']))

df1.show()
