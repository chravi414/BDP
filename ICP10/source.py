from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

spark = SparkSession \
    .builder \
    .appName("BDP ICP 10") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# PART - 1

# Import the dataset and create data framesdirectly on import
df = spark.read.csv("survey.csv", header=True)
df.show(5)

# Save data to file
df.write.format("csv").option("header", "false").save("result")

# Duplicate Records count and dropping
print(df.dropDuplicates().count())

df.groupBy(df.columns)\
    .count()\
    .where(fun.col('count') > 1)\
    .select(fun.sum('count'))\
    .show()


# Union Operation
df.createOrReplaceTempView("Survey")
spark.sql("select * from Survey where Gender = 'Male' or Gender = 'M' or Gender='male'").createTempView("Gender_Male")
spark.sql("select * from Survey where Gender = 'Female' or Gender = 'female'").createTempView("Gender_Female")
spark.sql("select * from Gender_Male UNION select * from Gender_Female order by Country").show()


# Group By Query on Treatment
spark.sql("select treatment,count(*) as count from Survey group by treatment").show()


# #### PART - 2

# Queries with Joins and Aggregate

spark.sql("select m.age,m.Country,m.Gender, m.treatment,f.Gender,f.treatment from Gender_Male m join Gender_Female f on m.Country = f.Country").show()

spark.sql(
    "select Country ,count(*) as COUNTRY_COUNT from Survey group by Country").show()

spark.sql(
    "select avg(Age) as AVG_AGE from Survey").show()


# Get 13th Row of Dataset
spark.sql("select * from Survey ORDER BY Timestamp limit 13").createTempView("Top13")
spark.sql("select * from Top13 order by Timestamp desc limit 1").show()
