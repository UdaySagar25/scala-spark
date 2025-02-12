import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SvSE {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("expression")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Creating DataFrame directly
    val df = Seq(
      (1, "Ajay", 300, 55.5F, 92.75),
      (2, "Bharghav", 350, 63.2F, 88.5),
      (3, "Chaitra", 320, 60.1F, 75.8),
      (4, "Kamal", 360, 75.0F, 82.3),
      (5, "Sohaib", 450, 70.8F, 90.6)
    ).toDF("Roll", "Name", "Final Marks", "Float Marks", "Double Marks")


//    df.show()

//    df.select($"Roll", $"Name", $"Final Marks").show()

//    df.selectExpr("Roll", "Name", "`Final Marks`").show()

    df.select($"Name",  ($"Float Marks" + 10).as("Updated Float Marks")).show()

    df.selectExpr("Name","`Float Marks` + 10 AS `Updated Float Marks`").show()

    spark.stop()
  }
}
