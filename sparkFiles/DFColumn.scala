import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DFColumn {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("DFCOlumn")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df: DataFrame=Seq(
      (1, "Ajay", 55),
      (2, "Bharghav", 63),
      (3, "Chaitra", 60),
      (4, "Kamal", 75),
      (5, "Sohaib", 70)
    ).toDF("Roll", "Name", "Marks")

    df.show()

    // Adding a new column to the created dataframe
    val add_column=df.withColumn("Updated Marks", col("Marks")+5)
    add_column.show()

    // Renaming an existing column
    val rename_column= df.withColumnRenamed("Roll", "Roll Number")
    rename_column.show()

    val select_columns = df.select("Name", "Marks")
    select_columns.show()


    // Dropping an existing column
    val drop_column=df.drop("Roll")
    drop_column.show()

    // Filtering Based on Column Values
    val filter_column=add_column.filter(col("Updated Marks")>=65)
    filter_column.show()

    // Creating columns with conditional values
    val dfCategory=add_column.withColumn("Division", when(col("Updated Marks")>70, "Distinction").otherwise("First class"))
    dfCategory.show()

    spark.stop()
  }
}
