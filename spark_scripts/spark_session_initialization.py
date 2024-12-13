from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col
from pyspark.sql import Row 

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RecommendationSystem") \
    .enableHiveSupport() \
    .getOrCreate()

df=spark.sql("show databases") 
df.show()