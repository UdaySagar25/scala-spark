import org.apache.spark.sql.SparkSession

object csvOps5 {
  def main(args: Array[String]): Unit={
    val spark=SparkSession.builder()
      .appName("Handling unescaped quotes in csv files")
      .master("local[*]")
      .getOrCreate()

    val dfDelimiter = spark.read
      .option("header", "true")
      .option("unescapedQuoteHandling", "STOP_AT_CLOSING_QUOTE")
      .csv("csvFiles/delimiter.csv")

    dfDelimiter.show(truncate = false)

    val dfDelimiter1 = spark.read
      .option("header", "true")
      .option("unescapedQuoteHandling", "BACK_TO_DELIMITER")
      .csv("csvFiles/delimiter.csv")

    dfDelimiter1.show(truncate = false)

    val dfDelimiter2 = spark.read
      .option("header", "true")
      .option("unescapedQuoteHandling", "STOP_AT_DELIMITER")
      .csv("csvFiles/delimiter.csv")

    dfDelimiter2.show(truncate = false)

    spark.stop()
  }
}
