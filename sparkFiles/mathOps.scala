import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object mathOps{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("mathOps")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Creating DataFrame directly
    val df = Seq(
      (1.toShort, "Ajay", 300, 55.5F, 92.75),
      (2.toShort, "Bharghav",  350, 63.2F, 88.5),
      (3.toShort, "Chaitra", 320, 60.1F, 75.8),
      (4.toShort, "Kamal", 360, 75.0F, 82.3),
      (5.toShort, "Sohaib",  450, 70.8F, 90.6)
    ).toDF("Roll", "Name",  "Final Marks", "Float Marks", "Double Marks")

//    df.show()

    //adding the column values
    val sumCol=df.withColumn("Sum of scores", col("Float Marks")+col("Double Marks"))
//    sumCol.show()

    val diffCol=df.withColumn("Difference of scores", col("Double Marks")-col("Float Marks"))
//    diffCol.show()

    val product=df.withColumn("Updated scores", col("Float Marks")*1.5)
//    product.show()

    val division=df.withColumn("Updated scores", col("Final Marks")/2)
//    division.show()

    // // Aggregate Functions
//    df.agg(sum("Final Marks").alias("Total Score")).show()

//    df.agg(avg("Float Marks").alias("Average Score")).show()

    df.agg(min("Double Marks").alias("Minimum Score")).show()
    df.agg(max("Float Marks").alias("Maximum Score")).show()


    spark.stop()
  }
}
