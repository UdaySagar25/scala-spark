import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
object rowOperations {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("rowOperations")
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

//    df.show()

    // adding new row to the dataframe
    val newRow=Seq((6,"Tanmay", 77)).toDF("Roll", "Name", "Marks")
    val updatedDF=df.union(newRow)
//    updatedDF.show()

    // adding multiple new rows to the dataframe
    val multipleRows=Seq((7,"Tina",67),
      (8,"Utkarsh",65)).toDF("Roll", "Name", "Marks")
    val moreRows=updatedDF.union(multipleRows)
//    moreRows.show()

    val delRow = df.filter(!($"Marks" <=63))
//    delRow.show()


    val rmRows = updatedDF.filter(expr("Marks % 2 = 0"))
    val newDf=updatedDF.except(rmRows)
//    newDf.show()

//    moreRows.distinct().show()

    val descendingOrder=moreRows.orderBy(col("Marks").desc)
    descendingOrder.show()

    val ascendingOrder=moreRows.orderBy(col("Marks").asc)
    ascendingOrder.show()

    spark.stop()
  }
}
