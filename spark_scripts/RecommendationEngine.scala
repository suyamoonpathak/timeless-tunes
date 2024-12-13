import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.RegressionEvaluator

object RecommendationEngine {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session with Hive support
    val spark = SparkSession.builder
      .appName("Recommendation Engine")
      .enableHiveSupport()
      .getOrCreate()

    // Load data from Hive tables into Spark DataFrames
    val combinedSongData = spark.sql("SELECT * FROM combined_song_data")
    val songGrouped = spark.sql("SELECT * FROM song_grouped")

    // Task 1: Train ALS Model
    val alsModel = trainALSModel(combinedSongData)

    // Task 2: Generate Recommendations
    val userData: DataFrame = ... // Prepare data for users you want to generate recommendations for
    val numRecommendations: Int = 10 // Number of recommendations to generate per user
    val recommendations = generateRecommendations(alsModel, userData, numRecommendations)
    recommendations.show()

    // Task 3: Evaluate ALS Model (Optional)
    val testData: DataFrame = ... // Prepare your test data
    val rmse: Double = evaluateALSModel(alsModel, testData)
    println(s"RMSE: $rmse")

    // Stop Spark session
    spark.stop()
  }

  // Train the ALS model
  def trainALSModel(trainingData: DataFrame): ALSModel = {
    val als = new ALS()
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("rating")
    
    als.fit(trainingData)
  }

  // Generate recommendations for a given set of users
  def generateRecommendations(model: ALSModel, userData: DataFrame, numRecommendations: Int): DataFrame = {
    val userSubsetRecs = model.recommendForUserSubset(userData, numRecommendations)
    userSubsetRecs
      .withColumn("recommendations", explode($"recommendations"))
      .select($"user_id", $"recommendations.item_id".alias("recommended_item_id"), $"recommendations.rating")
  }

  // Evaluate the ALS model
  def evaluateALSModel(model: ALSModel, testData: DataFrame): Double = {
    val predictions = model.transform(testData)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    
    val rmse = evaluator.evaluate(predictions)
    rmse
  }
}
