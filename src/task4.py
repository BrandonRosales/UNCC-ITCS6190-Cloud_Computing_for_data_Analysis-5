from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum as spark_sum

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load posts and users data
posts_df = (
	spark.read.option("header", True).csv("/input/posts.csv")
	.select(col("UserID"), col("Likes"), col("Retweets"))
)
users_df = spark.read.option("header", True).csv("/input/users.csv").select(col("UserID"), col("Username"), col("Verified"))

# Cast numeric fields and filter only verified users
posts_clean = (
	posts_df.withColumn("UserID", col("UserID").cast("int"))
	.withColumn("Likes", col("Likes").cast("int"))
	.withColumn("Retweets", col("Retweets").cast("int"))
)
users_clean = users_df.withColumn("UserID", col("UserID").cast("int")).withColumn("Verified", col("Verified").cast("boolean"))

# Join and aggregate engagement for verified users
verified_posts = posts_clean.join(users_clean, on="UserID", how="inner").where(col("Verified") == True)

top_verified_users = (
	verified_posts.withColumn("Engagement", col("Likes") + col("Retweets"))
	.groupBy("Username")
	.agg(
		count("*").alias("PostCount"),
		spark_sum("Likes").alias("TotalLikes"),
		spark_sum("Retweets").alias("TotalRetweets"),
		spark_sum("Engagement").alias("TotalEngagement"),
	)
	.orderBy(desc("TotalEngagement"), desc("PostCount"), col("Username"))
)

# Save result
output_path = "/outputs/top_verified_users"
top_verified_users.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# Stop Spark
spark.stop()
