import org.apache.spark.sql.{DataFrame, SparkSession}

object convertCSV {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("convertCSV")
      .master("local[*]")
      .getOrCreate()

    // Read CSV File into DataFrame
    val df: DataFrame = spark.read
      .option("header", "true")  // Use first row as column names
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv("marks.csv")

    df.show()
    spark.stop()
  }
}
