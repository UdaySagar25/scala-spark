import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types._

object csvOps4 {
  def main(args: Array[String]): Unit={
    System.load("C:\\Users\\krisa\\OneDrive\\Desktop\\repos\\winutils\\hadoop-3.3.5\\bin\\hadoop.dll")
    val spark=SparkSession.builder()
      .appName("Writing CSV files from a dataframe")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()

    import spark.implicits._

    val df=Seq(
      (1.toShort, "Ajay", 300, 55.5F, 92.75),
      (2.toShort, "Bharghav",  350, 63.2F, 88.5),
      (3.toShort, "Chaitra", 320, 60.1F, 75.8),
      (4.toShort, "Kamal", 360, 75.0F, 82.3),
      (5.toShort, "Sohaib",  450, 70.8F, 90.6)
      ).toDF("Roll", "Name",  "Final Marks", "Float Marks", "Double Marks")

    df.show()
//    df.printSchema()

//    df.write.format("csv")
//      .option("header","true")
//      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\outputFiles1")


//    df.coalesce(1).write.format("csv")
//      .option("header","true")
//      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\singleFile")

//    df.coalesce(2).write.format("csv")
//      .option("header","true")
//      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\outputFiles1")

//    df.coalesce(2).write.format("csv")
//      .option("header","true")
//      .mode("overwrite")
//      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\outputFiles1")


//    df.coalesce(1).write.format("csv")
//      .option("header","true")
//      .option("delimiter","|")
//      .mode("overwrite")
//      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\customDelimiter")

    df.coalesce(1).write.format("csv")
      .option("header","true")
      .option("delimiter",";")
      .mode("append")
      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\customDelimiter")

    spark.stop()
  }
}
