
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test5 {

  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create an empty DataFrame with an initial schema
    var df = spark.emptyDataFrame
    println("Initial empty DataFrame created.")

    // Start measuring execution time
    val startTime = System.currentTimeMillis()

    // Loop to modify DataFrame
    for (i <- 1 to 25) {
      // Add a new column with null values
      df = df.withColumn(s"col$i", lit(null))

      // Filter the DataFrame
      df = df.filter($"col$i".isNotNull).cache()

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