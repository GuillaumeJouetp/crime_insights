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

number_of_crimes_by_category = police_df.groupby('Category').agg(count('Category').alias('count')).orderBy('count', ascending=False)
display(number_of_crimes_by_category)

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
display(police_df.select('PdDistrict', 'Category').groupby('PdDistrict', 'Category').agg(count('PdDistrict').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# A more deltailed look at where the drug/narcotic crimes take place
display(police_df.filter("Category='DRUG/NARCOTIC'").select('PdDistrict', 'Category').groupby('Category', 'PdDistrict').agg(count('PdDistrict').alias('count')))

# COMMAND ----------

# Register DataFrame as an SQL table
sqlContext.sql("DROP TABLE IF EXISTS insight_dataset")
dbutils.fs.rm("dbfs:/user/hive/warehouse/insight_dataset", True)
sqlContext.registerDataFrameAsTable(police_df, "insight_dataset")

df = sqlContext.table("insight_dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Number of crimes within the top 10 categories in correlation to the district - Better view
# MAGIC -- Look as a bar chart and add the values to the chart
# MAGIC select count(CASE WHEN Category = "LARCENY/THEFT" THEN 1 END) as Theft,
# MAGIC count(CASE WHEN Category = "OTHER OFFENSES" THEN 1 END) as Other,
# MAGIC count(CASE WHEN Category = "NON-CRIMINAL" THEN 1 END) as NonCriminal,
# MAGIC count(CASE WHEN Category = "ASSAULT" THEN 1 END) as Assault,
# MAGIC count(CASE WHEN Category = "DRUG/NARCOTIC" THEN 1 END) as Drug,
# MAGIC count(CASE WHEN Category = "VANDALISM" THEN 1 END) as Vandalism,
# MAGIC count(CASE WHEN Category = "WARRANTS" THEN 1 END) as Warrants,
# MAGIC count(CASE WHEN Category = "BURGLARY" THEN 1 END) as Burglary,
# MAGIC count(CASE WHEN Category = "SUSPICIOUS OCC" THEN 1 END) as Suspicious,
# MAGIC PdDistrict as District from insight_dataset
# MAGIC where PdDistrict is not null group by PdDistrict

# COMMAND ----------

# Police data set up

from geopandas import *
from shapely.geometry import Point
import pandas as pd
import matplotlib.pyplot as plt

police_data = pd.read_csv('/dbfs/FileStore/tables/Police_Department_Incident_Reports__Historical_2003_to_May_2018.csv')
police_data['geometry'] = police_data.apply(lambda row: Point(row['X'], row['Y']), axis=1)

# COMMAND ----------

# Drug activity display
drug_category = ['DRUG/NARCOTIC']

drug_police_data = police_data[police_data.Category.isin(drug_category)]

drug_geo_police_data = geopandas.GeoDataFrame(drug_police_data, geometry='geometry')
drug_geo_police_data.crs = {'init': 'epsg:4326'}
drug_geo_police_data.plot(figsize=(13,10), color='red', alpha=0.02)
plt.axis([-122.52, -122.36, 37.70, 37.83])

plt.show()
display()

# COMMAND ----------

# Suspicious activity display
suspicious_occ_category = ['SUSPICIOUS OCC']

suspicious_police_data = police_data[police_data.Category.isin(suspicious_occ_category)]

suspicious_geo_police_data = geopandas.GeoDataFrame(suspicious_police_data, geometry='geometry')
suspicious_geo_police_data.crs = {'init': 'epsg:4326'}
suspicious_geo_police_data.plot(figsize=(13,10), color='green', alpha=0.02)
plt.axis([-122.52, -122.36, 37.70, 37.83])

plt.show()
display()

# COMMAND ----------

# San Francisco Map Display

geo_map = geopandas.read_file('/dbfs/FileStore/tables/geojson.json')

geo_map.crs = {'init': 'epsg:4326'}
geo_map = geo_map.rename(columns={'geometry': 'geometry','nhood':'neighborhood_name'}).set_geometry('geometry')

geo_map.plot(figsize=(13,10), color='gray')
plt.show()

display()

# COMMAND ----------

# Final result Display

fig, ax = plt.subplots(1, figsize=(13,10))
sf_map = geo_map.plot(ax=ax, color='gray')
drug_geo_police_data.plot(ax=sf_map, marker="o", color="red", markersize=8, edgecolor='k', alpha=0.05)
# suspicious_geo_police_data.plot(ax=sf_map, marker="o", color="green", markersize=8, edgecolor='k', alpha=0.05)  -- too much to see
ax.set_title("San Francisco Drug Crime Map")

plt.axis([-122.52, -122.36, 37.70, 37.83])
plt.show()
display()

# COMMAND ----------

# MAGIC %md ### Part 2: Categories of crimes likely to lead to an arrest ?

# COMMAND ----------

# An overview of the most common actions taken by the police of reported crimes
display(police_df.groupby('Resolution').agg(count('Resolution').alias('count')).orderBy('count', ascending=False))

# COMMAND ----------

# We define when we suppose that a crime led to an arrest
police_df_with_arrested_column = police_df.withColumn("Arrested?", when(col("Resolution").isin(['NONE','NOT PROSECUTED']),"NOT ARRESTED").otherwise("ARRESTED"))
police_df_with_arrested_column.cache()
display(police_df_with_arrested_column)

# COMMAND ----------

# Count number of arrests by category of crimes
df_number_of_arrested_by_category = police_df_with_arrested_column.select('Category', 'Arrested?').groupby('Arrested?', 'Category').agg(count('Arrested?').alias('Arrested?_count'))
display(df_number_of_arrested_by_category)

# COMMAND ----------

# Shows the categories of crime most likely to lead to an arrest
df_ordered_by_pourcentages = df_number_of_arrested_by_category.join(number_of_crimes_by_category, on=['Category'], how='inner').withColumn("pourcentage", col("count")/col("Arrested?_count")).orderBy(['Arrested?','pourcentage'], ascending=False)
display(df_ordered_by_pourcentages)

# COMMAND ----------

# Shows the first ten categories of crime most likely to lead to an arrest
slice_for_plot = ['PROSTITUTION', 'WARRANTS', 'DRIVING UNDER THE INFLUENCE', 'DRUG/NARCOTIC', 'LIQUOR LAWS', 'LOITERING', 'STOLEN PROPERTY','DRUNKENNESS', 'WEAPON LAWS', 'OTHER OFFENSES']
display(df_ordered_by_pourcentages.select('*').where(col('Category').isin(slice_for_plot)))

# COMMAND ----------

# Shows the last five categories of crime most likely to lead to an arrest
slice_for_plot = ['RECOVERED VEHICLE', 'VEHICLE THEFT', 'LARCENY/THEFT', 'SUSPICIOUS OCC', 'VANDALISM']
display(df_ordered_by_pourcentages.select('*').where(col('Category').isin(slice_for_plot)))

# COMMAND ----------

# Show what type of sex offences most likely to lead to an arrest
sex_offences = ['SEX OFFENSES, FORCIBLE', 'SEX OFFENSES, NON FORCIBLE']
display(df_ordered_by_pourcentages.select('*').where(col('Category').isin(sex_offences)))

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
display(police_df_dt.select('Hour', 'Category').groupby('Hour', 'Category').agg(count('Hour').alias('count')).orderBy('Hour', ascending=True))

# COMMAND ----------

# Frequency of crimes in correlation to time of day
# There is no particular category that noticeable stands out by this result at first glance,
# but it is interesting to see that the time of day with most reported crimes is 18.00

# .where(col('Category').isin(top_categories))
display(police_df_dt.select('Hour', 'Category').where(col('Category').isin(top_categories)).groupby('Hour', 'Category').agg(count('Hour').alias('count')).orderBy('Hour', ascending=True))

# COMMAND ----------

# Frequency of crimes in correlation to month year
# There is no particular category that noticeable stands out by this result at first glance,
display(police_df_dt.select('Month', 'Category').where(col('Category').isin(top_categories)).groupby('Month', 'Category').agg(count('Month').alias('count')).orderBy('Month', ascending=True))

# COMMAND ----------

# Frequency of crimes in correlation to day of month
# 31 is obviously less than the others as there is fewever 31th than the other days of month,
# however it is interesting that the 1th is higher than the others??
display(police_df_dt.select('DayOfMonth', 'Category').where(col('Category').isin(top_categories)).groupby('DayOfMonth', 'Category').agg(count('DayOfMonth').alias('count')).orderBy('DayOfMonth', ascending=True))

# COMMAND ----------

# Further work.....
