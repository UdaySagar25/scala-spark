import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

object stringOps {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("mathOps")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame=Seq(
      (1, "Ajay", "Physics","Hyderabad"),
      (2, "Bharghav","Cyber Security","Mumbai"),
      (3, "Chaitra","Material Science", "Indore"),
      (4, "Kamal","Design", "Puri"),
      (5, "Sohaib","Nuclear Science", "Cochin")
    ).toDF("Roll", "Name","Dept", "Location")

//    df.show()

    //convert the name to lowercases
    val lowerDf=df.withColumn("Lower Case", lower(col("Name")))
//    lowerDf.show()

    val upperDf=df.withColumn("Upper Case", upper(col("Dept")))
//        upperDf.show()

    val dfTrim = df.withColumn("Dept Trimmed", trim(col("Dept")))
//    dfTrim.show()

    val dfLength = df.withColumn("Name Length", length(col("Name")))
//    dfLength.show()

    val dfReverse = df.withColumn("Dept Reverse", reverse(col("Dept")))
//    dfReverse.show()


    val dfSubstring = df.withColumn("Name Substring", substring(col("Name"), 1, 3))
//    dfSubstring.show()

    // Concatenating two columns
    val dfConcat = df.withColumn("Name-City", concat(col("Name"), lit(" - "), col("Location")))
    dfConcat.show()
 
    spark.stop()
  }
}
