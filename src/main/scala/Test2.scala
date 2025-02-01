
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test2 {

  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    // Set the Checkpoint directory
    spark.sparkContext.setCheckpointDir("checkpoints")

    import spark.implicits._

    // Create an empty DataFrame with an initial schema
    var df = spark.emptyDataFrame
    println("Initial empty DataFrame created.")

    // Start measuring execution time
    val startTime = System.currentTimeMillis()

    // Loop to modify DataFrame
    for (i <- 1 to 25) {
      // Add a new column with null values
      df = df.withColumn(s"col$i", lit(null)).cache()

      // Filter the DataFrame
      df = df.filter($"col$i".isNotNull)

      // Checkpoint the DataFrame
      df = df.checkpoint()

      // Explain the query plan
      df.explain()

      // Show the DataFrame
      df.show()
    }

    // Print execution time
    val totalTime = System.currentTimeMillis() - startTime
    println(s"Total execution time: $totalTime ms")

    // Stop the SparkSession
    spark.stop()
    println("SparkSession stopped.")
  }
}