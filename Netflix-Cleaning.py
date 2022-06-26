import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col

#Create spark session
spark=SparkSession.builder.appName('DataFrame').getOrCreate()

#Reading csv file
df_pyspark = spark.read.csv('movies.csv', multiLine=True, header=True,inferSchema=True, sep=',')

#Creating a seperate dataframe to work on
df_pyspark_alter = df_pyspark.alias('df_pyspark_alter')


regex_string_parenthesis = "(\\(|\\))"
regex_string_newline = "(\n)"

#Parenthesis and newline replacements through regex
df_pyspark_alter=df_pyspark_alter.withColumn("YEAR_cleaned",regexp_replace(col("YEAR"), regex_string_parenthesis,"")) \
                .withColumn("GENRE-cleaned",regexp_replace(col("GENRE"), regex_string_newline,"")) \
                .withColumn("ONE-LINE-cleaned",regexp_replace(col("ONE-LINE"), regex_string_newline,"")) \
                .withColumn("STARS-cleaned",regexp_replace(col("STARS"), regex_string_newline,""))

print(df_pyspark_alter.show(10))


spark.stop()