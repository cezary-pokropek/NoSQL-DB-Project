from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession\
    .builder\
    .appName('data_processing_spark_job')\
    .master('local[4]')\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .config("spark.sql.session.timeZone", "UTC")\
    .getOrCreate()

data_schema = StructType([
    StructField("bookID", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("authors", StringType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("isbn", StringType(), True),
    StructField("isbn13", StringType(), True),
    StructField("language_code", StringType(), True),
    StructField("num_pages", IntegerType(), True),
    StructField("ratings_count", IntegerType(), True),
    StructField("text_reviews_count", IntegerType(), True),
    StructField("publication_date", StringType(), True),
    StructField("publisher", StringType(), True)
])

raw_data = spark\
    .read\
    .schema(data_schema)\
    .csv(f'data/books.csv')

data = raw_data\
    .withColumn('authors', split(col('authors'), '/'))\
    .withColumn('publication_date', to_date(col('publication_date'), 'M/d/yyyy'))

data\
    .write\
    .format("mongo")\
    .mode("overwrite")\
    .option("uri", "mongodb://localhost:27017/")\
    .option("database", "local")\
    .option("collection", "books")\
    .save()
