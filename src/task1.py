from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data from CSV with header
posts_df = spark.read.option("header", True).csv("/input/posts.csv")


hashtags = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))

hashtag_counts = (
	hashtags.where(col("Hashtag") != "")
	.groupBy("Hashtag")
	.count()
	.orderBy(desc("count"), col("Hashtag"))
)

output_path = "/outputs/hashtag_trends"
hashtag_counts.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# Stop Spark session
spark.stop()
