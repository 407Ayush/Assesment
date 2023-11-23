# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
#reading csv data as df
df = spark.read.options(header='True', inferSchema='True').csv('dbfs:/FileStore/shared_uploads/nush407@gmail.com/database.csv')
#date column has different date formats. cnoverting all to a same format
df1 = df.withColumn("final_date", when(df["DATE"].contains("-"),date_format(to_date("DATE","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),"M/d/yyyy")).otherwise(date_format(to_date("DATE","M/d/yyy"),"M/d/yyyy")))
display(df1)

# selecting required column only
df2 = df1.select('final_date','Time','Latitude',	'Longitude', 'Magnitude','Location Source','Type','ID')

# creating new columns like year,month,day_of_month,day_of_week
df3 = df2.withColumn('Year', year(to_date('final_date',"M/d/yyyy"))).withColumn("day_of_week",dayofweek(to_date('final_date',"M/d/yyyy"))).withColumn("day_of_month",dayofmonth(to_date('final_date',"M/d/yyyy"))).withColumn('Month', month(to_date('final_date',"M/d/yyyy")))



# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
#1 How does the Day of a Week affect the number of earthquakes?
df_earthquake_freq_week = df3.groupBy('day_of_week').agg(count("*").alias("equake_per_weekday")).orderBy(asc('day_of_week'))
df_earthquake_freq_week.show()

#2 What is the relation between Day of the month and Number of earthquakes that happened in a year?
df_earthquake_day_of_month = df3.groupBy('year','day_of_month').agg(count("*").alias("equake_per_day_per_month")).orderBy(asc('year'))
df_earthquake_day_of_month.display()

#3 What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?
filtered_df = df3.filter((col('Year')>=1965) & (col('Year')<=2016))
monthly_count_df = filtered_df.groupBy('year','Month').agg(count('*'))
avg_freq = monthly_count_df.groupBy('Month').avg("count(1)")
avg_freq.display()

#4 What is the relation between Year and Number of earthquakes that happened in that year?
df_earthquake_freq_year = df3.groupBy('Year').agg(count("*").alias("equake_per_year")).orderBy(asc('Year'))
df_earthquake_freq_year.show()

#5 How has the earthquake magnitude on average been varied over the years?
avg_mag = df3.groupBy('Year').avg("Magnitude")
avg_mag.show()

#6 How does year impact the standard deviation of the earthquakes?
st_dev = df3.agg({'Year':'stddev'})
st_dev.show()  #used stddev function 

#7 Does geographic location have anything to do with earthquakes?
earthquake_location = df3.groupBy('Location Source').agg(count("*").alias("earthquake_count"))
earthquake_location.show()

#8 Where do earthquakes occur very frequently?
earthquake_max= earthquake_location.orderBy(desc('earthquake_count')).limit(1)
earthquake_max.display()
