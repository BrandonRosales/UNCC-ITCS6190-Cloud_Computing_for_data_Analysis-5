from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum

# Initialize Spark Session
spark = SparkSession.builder.appName("EngagementByAge").getOrCreate()

# Load posts and users data from CSV files
posts_df = (
	spark.read.option("header", True).csv("/input/posts.csv")
	.select(col("UserID"), col("Likes"), col("Retweets"))
)
users_df = spark.read.option("header", True).csv("/input/users.csv").select(col("UserID"), col("AgeGroup"))

# Cast numeric columns to integers and compute engagement per post
posts_clean = (
	posts_df.withColumn("UserID", col("UserID").cast("int"))
	.withColumn("Likes", col("Likes").cast("int"))
	.withColumn("Retweets", col("Retweets").cast("int"))
	.withColumn("Engagement", col("Likes") + col("Retweets"))
)

# Join posts with user metadata to aggregate by AgeGroup
engagement_by_age = (
	posts_clean.join(users_df.withColumn("UserID", col("UserID").cast("int")), on="UserID", how="inner")
	.groupBy("AgeGroup")
	.agg(
		avg("Likes").alias("AverageLikes"),
		avg("Retweets").alias("AverageRetweets"),
		avg("Engagement").alias("AverageEngagement"),
		spark_sum("Engagement").alias("TotalEngagement"),
	)
	.orderBy("AgeGroup")
)

# Save result
output_path = "/outputs/engagement_by_age"
engagement_by_age.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# Stop Spark
spark.stop()
