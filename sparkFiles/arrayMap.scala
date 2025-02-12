import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
object arrayMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("dateTime")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // create a dataframe with Array Type
    val df: DataFrame = Seq(
      (1, "Ajay", Array(55,65,72)),
      (2, "Bharghav", Array(63,79,71)),
      (3, "Chaitra", Array(60,63, 75)),
      (4, "Kamal", Array(73,75,69)),
      (5, "Sohaib", Array(86,74,70))
    ).toDF("Roll", "Name", "Marks")

//    df.show()

  //accessing array elements of the array
    val arrayElements=df.withColumn("Marks",$"Marks"(1))
//    arrayElements.show()

//    df.withColumn("Marks Length",size($"Marks")).show()

//    df.withColumn("Has Marks", array_contains($"Marks", 63)).show()

//    df.withColumn("Marks",explode($"Marks")).show()

//    df.withColumn("Marks", array_filter($"Marks", x =>x>65))

    val mapDf=Seq(
      (1, Map("name"->"Ajay", "Marks"->55)),
      (2, Map("name"->"Bharghav", "Marks"->63)),
      (3, Map("name"->"Chaitra", "Marks"->60)),
      (4, Map("name"->"Kamal", "Marks"->75)),
      (5, Map("name"->"Sohaib", "Marks"->70))
    ).toDF("Id", "Details")

    mapDf.show()

    spark.stop()
  }
}
