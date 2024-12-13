import org.apache.spark.sql._

object HiveIntegration {
  def main(args: Array[String]): Unit = {
    // Create SparkSession with Hive support
    val spark = SparkSession
      .builder()
      .appName("HiveIntegration")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    // Load a Hive table
    val df = spark.sql("SELECT * FROM song_data limit 10")

    // Perform operations on the loaded Hive table
    df.show()

    // Stop SparkSession
    spark.stop()
  }
}
