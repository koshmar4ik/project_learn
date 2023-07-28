import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

df = spark.read.option('inferSchema', True).option('header', True).csv("/home/sapsan/owid-covid-data.csv")
from pyspark.sql import functions as f
from pyspark.sql.window import Window
windowSpec2 = Window.partitionBy("location").orderBy(f.desc(f.col("new_cases")))
windowSpec3 = Window.orderBy("date")

def sumer():
	return df['iso_code','date','total_cases'].where(f.col('date').startswith("2020-03-31")).where(f.col('iso_code').startswith('OWID') == False).select(f.sum(f.col('total_cases'))).collect()[0][0]


df['iso_code','date','location','total_cases'].where(f.col('date').startswith("2020-03-31")).where(f.col('iso_code').startswith('OWID') == False).withColumn("percent",f.col('total_cases') / sumer() * 100).sort(df.total_cases.desc()).show(15)

df['iso_code','date','location','new_cases'].where(f.col('date').between("2021-03-25","2021-03-31")).where(f.col('iso_code').startswith('OWID') == False).withColumn("row_number",f.row_number().over(windowSpec2)).where(f.col('row_number') == 1).orderBy(f.desc(f.col('new_cases'))).show(10)

df['date','location','new_cases'].where(f.col('location').like('Russia')).where(f.col('date').between("2021-03-25","2021-03-31")).orderBy(f.col('date')).withColumn("yesterday",f.lag("new_cases",1).over(windowSpec3)).withColumn("dif",f.col('new_cases') - f.col('yesterday')).fillna(value=0).show(10)
