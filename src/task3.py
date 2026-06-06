from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentEngagement").getOrCreate()

# Load posts data
posts_df = (
	spark.read.option("header", True).csv("/input/posts.csv")
	.select(col("SentimentScore"), col("Likes"), col("Retweets"))
)

# Cast types and label sentiment
posts_clean = (
	posts_df.withColumn("SentimentScore", col("SentimentScore").cast("double"))
	.withColumn("Likes", col("Likes").cast("int"))
	.withColumn("Retweets", col("Retweets").cast("int"))
	.withColumn(
		"SentimentLabel",
		when(col("SentimentScore") >= 0.25, "Positive")
		.when(col("SentimentScore") <= -0.25, "Negative")
		.otherwise("Neutral"),
	)
	.withColumn("Engagement", col("Likes") + col("Retweets"))
)

# Aggregate engagement metrics by sentiment label
sentiment_engagement = (
	posts_clean.groupBy("SentimentLabel")
	.agg(
		avg("Likes").alias("AverageLikes"),
		avg("Retweets").alias("AverageRetweets"),
		avg("Engagement").alias("AverageEngagement"),
	)
	.orderBy("SentimentLabel")
)

# Save result
output_path = "/outputs/sentiment_engagement"
sentiment_engagement.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# Stop Spark
spark.stop()
