package warehouse

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import warehouse.models.{Amount, Position}

object Warehouse {

  val positionsPath: String = locateResource("/positions.csv")
  val amountsPath: String = locateResource("/amounts.csv")

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Warehouse Statistics")
      .config("spark.master", "local")
      .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    try {
      // Load required data from files, e.g. csv of json.
      val positionsDS: Dataset[Position] = readCsv(positionsPath, Position.schema, Position.apply)
      val amountsDS: Dataset[Amount] = readCsv(amountsPath, Amount.schema, Amount.apply)

      // Find the current amount for each position, warehouse, product.
//      val joined = positionsDS.joinWith(amountsDS, positionsDS("positionId") === amountsDS("positionId"), "left")
//      joined.show()

      val statistics = amountStatistics(amountsDS)

      statistics.show()

      val rdd = statistics.rdd

      println(rdd.toDebugString)

      // Find max, min, avg amounts for each warehouse and product.


    } finally {
      spark.stop()
    }
  }

  def locateResource(path: String): String = getClass.getResource(path).getPath

  def readCsv[T : Encoder](path: String, schema: StructType, toTypedFoo: Row => T): Dataset[T] = {
    spark.read
      .schema(schema)
      .option("mode", "FAILFAST") // wanna know about parsing problems
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("timestampFormat", "s") // epoch time in seconds
      .csv(path)
      .map(toTypedFoo)
  }

  def amountStatistics(amounts: Dataset[Amount]) = {
    amounts
      .sort("eventTime")
      .groupByKey(_.positionId)
      .agg(
        avg("amount").name("avg").as[java.math.BigDecimal],
        min("amount").name("min").as[java.math.BigDecimal],
        max("amount").name("max").as[java.math.BigDecimal]
      )
  }
}
