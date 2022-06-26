import pyspark
from pyspark.sql import SparkSession

#Create spark session
spark=SparkSession.builder.appName('DataFrame').getOrCreate()

#Reading csv file
df_pyspark = spark.read.csv('/Users/aaronchatrath/Documents/Pyspark/Netflix-Data-clean/movies.csv', multiLine=True, header=True,inferSchema=True, sep=',')

#Creating a seperate dataframe to work on
df_pyspark_alter = df_pyspark.alias('df_pyspark_alter')


spark.stop()