import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object handleCsv {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("handleCsv")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //reading a csv file
    val readCsv= spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv("students.csv")

//    readCsv.show()
//    readCsv.printSchema()

    val delimiterCsv= spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .option("delimiter",";")
      .csv("students1.csv")
//    delimiterCsv.show()

    val df: DataFrame= Seq(
      (1, "Ajay", 300, 55.5F, 92.75),
      (2, "Bharghav",  350, 63.2F, 88.5),
      (3, "Chaitra", 320, 60.1F, 75.8),
      (4, "Kamal", 360, 75.0F, 82.3),
      (5, "Sohaib",  450, 70.8F, 90.6)
    ).toDF("Roll", "Name",  "Final Marks", "Float Marks", "Double Marks")

//    df.show()
//    df.write.option("header","true").csv("stDetails.csv")
//
//    df.coalesce(1) // To save as a single CSV file
//      .write
//      .format("csv")
//      .mode("overwrite")
//      .option("header", "true")
//      .option("sep", ",")
//      .save("output")

    val selectVals= readCsv.filter("Marks >= 63")
//    selectVals.show()

    val selectCols=readCsv.select("Name","Marks")
//    selectCols.show()

    val colName=readCsv.withColumnRenamed("Marks", "Math Marks")
    colName.show()

    colName.write.format("csv")
      .mode("overwrite")
      .option("header","true")
      .csv("output")


    spark.stop()
  }
}
