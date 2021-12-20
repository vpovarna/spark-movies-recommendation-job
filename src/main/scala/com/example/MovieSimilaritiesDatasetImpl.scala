package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

object MovieSimilaritiesDatasetImpl {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  case class MoviesNames(movieID: Int, movieTitle: String)

  case class MoviePairs(movie1: Int, movie2: Int, rating1: Int, rating2: Int)

  case class MoviePairsSimilarity(
      movie1: Int,
      movie2: Int,
      score: Double,
      numPairs: Long
  )

  def computeCosineSimilarity(
      spark: SparkSession,
      data: Dataset[MoviePairs]
  ): Dataset[MoviePairsSimilarity] = {
    // Compute xx, xy and yy columns
    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating1"))
      .withColumn("yy", col("rating2") * col("rating2"))
      .withColumn("xy", col("rating1") * col("rating2"))

    // Compute numerator, denominator and numPairs columns
    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    // Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    import spark.implicits._
    val result = calculateSimilarity
      .withColumn(
        "score",
        when(col("denominator") =!= 0, col("numerator") / col("denominator"))
          .otherwise(null)
      )
      .select("movie1", "movie2", "score", "numPairs")
      .as[MoviePairsSimilarity]

    result
  }

  /** Get movie name by given movie id */
  def getMovieName(movieNames: Dataset[MoviesNames], movieId: Int): String = {
    val result = movieNames
      .filter(col("movieID") === movieId)
      .select("movieTitle")
      .collect()(0)

    result(0).toString
  }

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder
      .appName("MovieSimilarities")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    println("\nLoading movie names...")
    import spark.implicits._
    // Create a broadcast dataset of movieID and movieTitle.
    // Apply ISO-885901 charset
    val movieNames = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    // Load up movie data as dataset
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    val ratings = movies.select("userId", "movieId", "rating")

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    // Select movie pairs and rating pairs
    val moviePairs = ratings
      .as("ratings1")
      .join(
        ratings.as("ratings2"),
        $"ratings1.userId" === $"ratings2.userId" && $"ratings1.movieId" < $"ratings2.movieId"
      )
      .select(
        $"ratings1.movieId".alias("movie1"),
        $"ratings2.movieId".alias("movie2"),
        $"ratings1.rating".alias("rating1"),
        $"ratings2.rating".alias("rating2")
      )
      .as[MoviePairs]

    val moviePairSimilarities =
      computeCosineSimilarity(spark, moviePairs).cache()

    val scoreThreshold = 0.97
    val coOccurrenceThreshold = 50.0

    moviePairSimilarities
      .filter(col("score") > scoreThreshold)
      .filter(col("numPairs") > coOccurrenceThreshold)
      .groupBy(col("movie1"))
      .agg(
        collect_list(col("movie2")).as("recommended_movies"),
        collect_list(col("score")).as("scores"),
        collect_list(col("numPairs")).as("numPairs")
      )
      .show(truncate = false)
  }
}
