# Databricks notebook source
#Load in data
police_df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/Police_Department_Incident_Reports__Historical_2003_to_May_2018.csv')
police_df.cache() # Cache data for faster reuse

display(police_df)

# COMMAND ----------

# Explore the dataset:
from pyspark.sql.functions import col, when, count

display(police_df.groupby('Category').agg(f.count('Category').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# List of top 10 categories by frequency
top_categories = ['LARCENY/THEFT', 'OTHER OFFENSES', 'NON-CRIMINAL', 'ASSAULT', 'VEHICLE THEFT', 'DRUG/NARCOTIC', 'VANDALISM', 'WARRANTS', 'BURGLARY', 'SUSPICIOUS', 'OCC']

# COMMAND ----------

# Top 10 categories of crime by freqency
display(police_df.groupby('Category').agg(count('Category').alias('count')).orderBy('count', ascending=False).limit(10))
#display(police_df.where(col('Category').isin(top_categories)).groupby('Category').agg(count('Category').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Experimenting with plotting - not much interesting here
display(police_df.select('DayOfWeek', 'Category').where(col('Category').isin(top_categories)).groupby('DayOfWeek', 'Category').agg(f.count('DayOfWeek').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Some categories which shows different frequencies depending on the day of the week.
# Especially prostitution shows big gap in frequency depending on the day of the week. 
alternative_categories = ['PROSTITUTION', 'DRUNKENNESS', 'DRIVING UNDER THE INFLUENCE']

police_df_with_wkday = police_df.withColumn("wk_num", when(col("DayOfWeek")=="Monday", 1)
                                            .when(col("DayOfWeek")=="Tuesday", 2)
                                            .when(col("DayOfWeek")=="Wednesday", 3)
                                            .when(col("DayOfWeek")=="Thursday", 4)
                                            .when(col("DayOfWeek")=="Friday", 5)
                                            .when(col("DayOfWeek")=="Saturday", 6)
                                            .otherwise("7"))
display(police_df_with_wkday.select('DayOfWeek', 'Category', 'wk_num').where(col('Category').isin(alternative_categories)).groupby('DayOfWeek', 'Category', 'wk_num').agg(f.count('DayOfWeek').alias('count')).orderBy('wk_num', ascending=True))

# COMMAND ----------

# Categories of crime in correlation to location
# It is appearent that drug/narcotic crimes tend to be clustered around the 'Tenderloin' area.
display(police_df.select('PdDistrict', 'Category').groupby('PdDistrict', 'Category').agg(f.count('PdDistrict').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# A more deltailed look at where the drug/narcotic crimes take place
display(police_df.filter("Category='DRUG/NARCOTIC'").select('PdDistrict', 'Category').groupby('Category', 'PdDistrict').agg(f.count('PdDistrict').alias('count')))

# COMMAND ----------

# An overview of the most common actions taken by the police of reported crimes
display(police_df.groupby('Resolution').agg(f.count('Resolution').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Shows the categories of crime most likely to lead to an arrest (may use some work as i didnt look at all the resolutions)
police_df2 = police_df.withColumn("Arrested?", when(col("Resolution").isin(['NONE','NOT PROSECUTED']),"NOT ARRESTED").otherwise("ARRESTED"))

display(police_df2.select('Arrested?', 'Category').where(col('Category').isin(top_categories)).groupby('Arrested?', 'Category').agg(f.count('Arrested?').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# A quick look at the Date and Time column
display(police_df.select('Date', 'Time'))

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import udf, year, month, dayofmonth, dayofyear, minute
from pyspark.sql.types import DateType


# Split the Date and Time columns into multiple smaller columns
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
police_df3 = police_df.withColumn('Date', func(col('Date')))
police_df4 = police_df3.withColumn('Year', year("Date"))
police_df4 = police_df4.withColumn('Month', month("Date"))
police_df4 = police_df4.withColumn('DayOfMonth', dayofmonth("Date"))
police_df4 = police_df4.withColumn('DayOfYear', dayofyear("Date"))
police_df4 = police_df4.withColumn('Hour', hour("Time"))

# Season based on "day of year" ranges for the northern hemisphere
police_df4 = police_df4.withColumn("Season", when(col("DayOfYear")>=264, 'fall')
                                            .when(col("DayOfYear")>=172, 'summer')
                                            .when(col("DayOfYear")>=80, 'spring')
                                            .otherwise('winter'))

display(police_df4)

# COMMAND ----------

# A look at if there is any difference in crime categories based on the season.
# It does not look like there is any difference,
# however we see that winter has slightly less crimes overall
display(police_df4.select('Season', 'Category').where(col('Category').isin(top_categories)).groupby('Season', 'Category').agg(count('Season').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Another view of the same result as above
display(police_df4.select('Season', 'Category').where(col('Category').isin(top_categories)).groupby('Season', 'Category').agg(count('Season').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# Frequency of crimes in correlation to time of day
# There is no particular category that noticeable stands out by this result at first glance,
# but it is interesting to see that the time of day with most reported crimes is 18.00
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

# COMMAND ----------


