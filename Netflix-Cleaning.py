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

#Splitting the 'Stars' original column into two columns - Director and Stars
#Selecting the cleaned columns for the netflix table 
split_cols = pyspark.sql.functions.split(df_pyspark_alter['STARS-cleaned'], 'Stars:')
df_pyspark_alter = df_pyspark_alter.select("MOVIES", "RATING", "VOTES", "RunTime", "YEAR_cleaned","GENRE-cleaned","ONE-LINE-cleaned","STARS-cleaned",
                split_cols.getItem(0).alias('Director'),
                split_cols.getItem(1).alias('Stars'))

#Dropping the original Stars column
df_pyspark_alter = df_pyspark_alter.drop('STARS-cleaned')

#Replacing string and character from Director column
regex_string_Dir_clean = "(\|)|Director:"
df_pyspark_alter=df_pyspark_alter.withColumn("Director-cleaned",regexp_replace(col("Director"), regex_string_Dir_clean,""))

#Further dropping of column not needed
df_pyspark_alter = df_pyspark_alter.drop('Director')

#Making table pretty with better namings
df_pyspark_alter = df_pyspark_alter.withColumnRenamed("MOVIES", "Movies") \
                    .withColumnRenamed("RATING", "Rating") \
                    .withColumnRenamed("VOTES", "Votes") \
                    .withColumnRenamed("YEAR_cleaned", "Year") \
                    .withColumnRenamed("GENRE-cleaned", "Genre") \
                    .withColumnRenamed("ONE-LINE-cleaned", "Summary") \
                    .withColumnRenamed("Director-cleaned", "Director")

#Dropping records if there are any missing records from the subset columns
df_pyspark_alter=df_pyspark_alter.na.drop("any", subset=["Rating", "Votes", "RunTime", "Year", "Genre"])

print(df_pyspark_alter.show(50))

spark.stop()