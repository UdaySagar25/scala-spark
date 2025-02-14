import org.apache.spark.sql.SparkSession

object csvOps1 {
  def main(args: Array[String]): Unit={
    val spark=SparkSession.builder()
      .appName("csvOps1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df=spark.read.option("header","true")
      .csv("csvFiles/multiline.csv")

//    df.show(truncate=false)

    val multilineDf=spark.read.option("header","true").option("multiLine","true")
      .csv("csvFiles/multiline.csv")

//    multilineDf.show(truncate =false)

    val lineSepCsv=spark.read.option("header","true").option("lineSep","\r\n")
      .csv("csvFiles/students2.csv")

//    lineSepCsv.show()

    val lineSepCsv1=spark.read.option("header","true").option("lineSep","\n")
      .csv("csvFiles/students2.csv")

//    lineSepCsv1.show()


    val dfDelimiter = spark.read
      .option("header", "true")
      .option("unescapedQuoteHandling", "BACK_TO_DELIMITER")
      .csv("csvFiles/delimiter.csv")

    dfDelimiter.show(truncate = false)


    spark.stop()
  }
}
