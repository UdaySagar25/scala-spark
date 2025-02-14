import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object csvOps2 {
  def main(args:Array[String]): Unit={
    val spark=SparkSession.builder()
      .appName("csvOps2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df=spark.read.option("inferSchema","true").option("header","true")
      .csv("csvFiles/students.csv")

//    df.show()
//    df.printSchema()
    val defineSchema = StructType(Seq(
      StructField("Roll", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Marks", DoubleType, true)
    ))

    val df1=spark.read.schema(defineSchema).option("header","true")
      .csv("csvFiles/students.csv")

//    df1.show()
//    df1.printSchema()

    val enfSchema = StructType(Seq(
      StructField("Roll", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Marks", FloatType, true)
    ))

    val df2=spark.read.option("enforceSchema", "true")
      .schema(enfSchema)
      .option("header","true")
      .csv("csvFiles/students.csv")

    df2.show()
    df2.printSchema()


    spark.stop()
  }
}
