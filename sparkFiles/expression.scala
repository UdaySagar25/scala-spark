import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object expression {
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

    // adding 2 marks to Double marks using select method
//    df.select($"Name",$"Double Marks", $"Double Marks" + 5 as ("Updated Marks")).show()

//    df.select($"Name", upper($"Name").as("Updated Name")).show()

//    df.filter( $"Final Marks" > 330 && $"Float Marks" >=70.0 ).show()

    val aggregate=df.agg(sum("Final Marks").as("Total Makrs"),avg("Float Marks").as("Average Marks"),
      min("Double Marks").as("Minimum Marks"))
//    aggregate.show()

    val updateExpr=df.select($"Name", $"Float Marks",expr("`Float Marks` * 1.5 as `Updated Marks`"))
    updateExpr.show()

    spark.stop()
  }
}
