from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date, explode, collect_set, sum, avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

MONGODB_URI = 'mongodb://localhost:27017/'
MONGODB_DB = 'bgd'

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

raw_books = spark\
    .read\
    .csv('data/books.csv', schema=data_schema, header=True)

books = raw_books\
    .withColumnRenamed('bookID', 'book_id')\
    .withColumn('authors', split(col('authors'), '/'))\
    .withColumn('publication_date', to_date(col('publication_date'), 'M/d/yyyy'))\
    .persist()

authors = books\
    .withColumn('author', explode('authors'))\
    .groupby('author')\
    .agg(
        collect_set('book_id').alias('book_ids'),
        collect_set('publisher').alias('publishers'),
        avg('average_rating').alias('books_average_rating'),
        avg('num_pages').alias('books_num_pages_avg'),
        sum('ratings_count').alias('books_ratings_count_sum'),
        sum('text_reviews_count').alias('books_text_reviews_count_sum'))

books\
    .write\
    .format("mongo")\
    .mode("overwrite")\
    .option("uri", MONGODB_URI)\
    .option("database", MONGODB_DB)\
    .option("collection", "books")\
    .save()

authors \
    .write \
    .format("mongo") \
    .mode("overwrite") \
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DB) \
    .option("collection", "authors") \
    .save()
