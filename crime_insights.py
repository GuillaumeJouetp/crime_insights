# Databricks notebook source
# MAGIC %md ### Part 0: Load the data.
# MAGIC 
# MAGIC The dataset we used is from "https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-Historical-2003/tmnf-yvry?fbclid=IwAR17xpAISe5iU9xqeDa16AuPB6cHp8ZJIOcXwvW9ncW-El6GTfNhOmDtEhg"

# COMMAND ----------

#Load in data
police_df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/Police_Department_Incident_Reports__Historical_2003_to_May_2018.csv')
police_df.cache() # Cache data for faster reuse

display(police_df)

# COMMAND ----------

# MAGIC %md ### Part 1: How categories of crimes are correlated to location ?

# COMMAND ----------

# Explore the dataset:
from pyspark.sql.functions import col, when, count

display(police_df.groupby('Category').agg(count('Category').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# List of top 10 categories by frequency
top_categories = ['LARCENY/THEFT', 'OTHER OFFENSES', 'NON-CRIMINAL', 'ASSAULT', 'VEHICLE THEFT', 'DRUG/NARCOTIC', 'VANDALISM', 'WARRANTS', 'BURGLARY', 'SUSPICIOUS OCC']
second_top_categories = ['MISSING PERSON', 'ROBBERY', 'FRAUD', 'SECONDARY CODES', 'FORGERY/COUNTERFEITING', 'WEAPON LAWS', 'TRESPASS', 'PROSTITUTION', 'STOLEN PROPERTY', 'SEX OFFENSES, FORCIBLE']
third_top_categories = ['DISORDERLY CONDUCT', 'DRUNKENNESS', 'RECOVERED VEHICLE', 'DRIVING UNDER THE INFLUENCE', 'KIDNAPPING', 'RUNAWAY', 'LIQUOR LAWS', 'ARSON', 'EMBEZZLEMENT', 'LOITERING']
fourth_top_categories = ['FAMILY OFFENSES', 'BAD CHECKS', 'BRIBERY', 'EXTORTION', 'SEX OFFENSES, NON FORCIBLE', 'GAMBLING', 'PORNOGRAPHY/OBSCENE MAT', 'TREA']

# COMMAND ----------

# Top 10 categories of crime by freqency
display(police_df.groupby('Category').agg(count('Category').alias('count')).orderBy('count', ascending=False).limit(10))
#display(police_df.where(col('Category').isin(top_categories)).groupby('Category').agg(count('Category').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Categories of crime in correlation to location
# It is appearent that drug/narcotic crimes tend to be clustered around the 'Tenderloin' area.
display(police_df.select('PdDistrict', 'Category').groupby('PdDistrict', 'Category').agg(f.count('PdDistrict').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# A more deltailed look at where the drug/narcotic crimes take place
display(police_df.filter("Category='DRUG/NARCOTIC'").select('PdDistrict', 'Category').groupby('Category', 'PdDistrict').agg(count('PdDistrict').alias('count')))

# COMMAND ----------

# MAGIC %md ### Part 2: Categories of crimes likely to lead to an arrest ?

# COMMAND ----------

# An overview of the most common actions taken by the police of reported crimes
display(police_df.groupby('Resolution').agg(count('Resolution').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Shows the categories of crime most likely to lead to an arrest (may use some work as i didnt look at all the resolutions)
police_df_with_arrested_column = police_df.withColumn("Arrested?", when(col("Resolution").isin(['NONE','NOT PROSECUTED']),"NOT ARRESTED").otherwise("ARRESTED"))
police_df_with_arrested_column.cache()

display(police_df_with_arrested_column.select('Arrested?', 'Category').where(col('Category').isin(top_categories)).groupby('Arrested?', 'Category').agg(count('Arrested?').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

likely_arrested = ['PROSTITUTION', 'STOLEN PROPERTY', 'WEAPON LAWS', 'DRUNKENNESS', 'DRIVING UNDER THE INFLUENCE', 'LIQUOR LAWS', 'LOITERING']

display(police_df_with_arrested_column.select('Arrested?', 'Category').where(col('Category').isin(likely_arrested)).groupby('Arrested?', 'Category').agg(count('Arrested?').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

sex_offences = ['SEX OFFENSES, FORCIBLE', 'SEX OFFENSES, NON FORCIBLE']

display(police_df_with_arrested_column.select('Arrested?', 'Category').where(col('Category').isin(sex_offences)).groupby('Arrested?', 'Category').agg(count('Arrested?').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# MAGIC %md ### Part 3: Show correlation between season/month/weekday/hour

# COMMAND ----------

# A quick look at the Date and Time column
display(police_df.select('Date', 'Time'))

# COMMAND ----------

# Experimenting with plotting - not much interesting here
display(police_df.select('DayOfWeek', 'Category').where(col('Category').isin(top_categories)).groupby('DayOfWeek', 'Category').agg(count('DayOfWeek').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Some categories which shows different frequencies depending on the day of the week.
# Especially prostitution shows big gap in frequency depending on the day of the week. 
alternative_categories = ['DRUNKENNESS', 'DRIVING UNDER THE INFLUENCE']

police_df_with_wkday = police_df.withColumn("wk_num", when(col("DayOfWeek")=="Monday", 1)
                                            .when(col("DayOfWeek")=="Tuesday", 2)
                                            .when(col("DayOfWeek")=="Wednesday", 3)
                                            .when(col("DayOfWeek")=="Thursday", 4)
                                            .when(col("DayOfWeek")=="Friday", 5)
                                            .when(col("DayOfWeek")=="Saturday", 6)
                                            .otherwise("7"))
police_df_with_wkday.cache()
display(police_df_with_wkday.select('DayOfWeek', 'Category', 'wk_num').where(col('Category').isin(alternative_categories)).groupby('DayOfWeek', 'Category', 'wk_num').agg(count('DayOfWeek').alias('count')).orderBy('wk_num', ascending=True))

# COMMAND ----------

# Some categories which shows different frequencies depending on the day of the week.
# Especially prostitution shows big gap in frequency depending on the day of the week. 
alternative_categories = ['PROSTITUTION', 'DRUG/NARCOTIC']

display(police_df_with_wkday.select('DayOfWeek', 'Category', 'wk_num').where(col('Category').isin(alternative_categories)).groupby('DayOfWeek', 'Category', 'wk_num').agg(count('DayOfWeek').alias('count')).orderBy('wk_num', ascending=True))

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import udf, year, month, dayofmonth, dayofyear, hour
from pyspark.sql.types import DateType


# Split the Date and Time columns into multiple smaller columns: Year, Month, DayOfMonth, DayOfYear, Hour, Season
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
police_df_dt = police_df.withColumn('Date', func(col('Date'))).withColumn('Year', year("Date")).withColumn('Month', month("Date")).withColumn('DayOfMonth', dayofmonth("Date")).withColumn('DayOfYear', dayofyear("Date")).withColumn('Hour', hour("Time")).withColumn("Season", when(col("DayOfYear")>=264, 'fall')
                                            .when(col("DayOfYear")>=172, 'summer')
                                            .when(col("DayOfYear")>=80, 'spring')
                                            .otherwise('winter'))
police_df_dt.cache()

display(police_df_dt)

# COMMAND ----------

# A look at if there is any difference in crime categories based on the season.
# It does not look like there is any difference,
# however we see that winter has slightly less crimes overall
display(police_df_dt.select('Season', 'Category').groupby('Season', 'Category').agg(count('Season').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Another view of the same result as above
display(police_df_dt.select('Season', 'Category').where(col('Category').isin(top_categories)).groupby('Season', 'Category').agg(count('Season').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

alternative_categories = ['PROSTITUTION', 'DRUG/NARCOTIC', 'DRUNKENNESS', 'DRIVING UNDER THE INFLUENCE']
display(police_df_dt.select('Season', 'Category').where(col('Category').isin(fourth_top_categories)).groupby('Season', 'Category').agg(count('Season').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Frequency of crimes in correlation to time of day
# There is no particular category that noticeable stands out by this result at first glance,
# but it is interesting to see that the time of day with most reported crimes is 18.00

# .where(col('Category').isin(top_categories))
display(police_df4.select('Hour', 'Category').groupby('Hour', 'Category').agg(count('Hour').alias('count')).orderBy('Hour', ascending=True))

# COMMAND ----------

# Frequency of crimes in correlation to time of day
# There is no particular category that noticeable stands out by this result at first glance,
# but it is interesting to see that the time of day with most reported crimes is 18.00

# .where(col('Category').isin(top_categories))
display(police_df4.select('Hour', 'Category').where(col('Category').isin(top_categories)).groupby('Hour', 'Category').agg(count('Hour').alias('count')).orderBy('Hour', ascending=True))

# COMMAND ----------

# Frequency of crimes in correlation to month year
# There is no particular category that noticeable stands out by this result at first glance,
display(police_df4.select('Month', 'Category').where(col('Category').isin(top_categories)).groupby('Month', 'Category').agg(count('Month').alias('count')).orderBy('Month', ascending=True))

# COMMAND ----------

# Frequency of crimes in correlation to day of month
# 31 is obviously less than the others as there is fewever 31th than the other days of month,
# however it is interesting that the 1th is higher than the others??
display(police_df4.select('DayOfMonth', 'Category').where(col('Category').isin(top_categories)).groupby('DayOfMonth', 'Category').agg(count('DayOfMonth').alias('count')).orderBy('DayOfMonth', ascending=True))

# COMMAND ----------

# Further work.....
