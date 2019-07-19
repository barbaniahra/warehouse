package warehouse

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import warehouse.models.{Amount, Position}
import warehouse.spark.LastValueByTimestamp

object Warehouse {
  type PositionStatistics = (Long, java.math.BigDecimal, java.math.BigDecimal, java.math.BigDecimal, java.math.BigDecimal)

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
      // Find max, min, avg amounts for each warehouse and product.

      val statistics = amountStatistics(amountsDS)

      val joined = positionsDS.joinWith(statistics,  positionsDS("positionId") === statistics("value"))

      joined.write.csv("current_amounts.csv")
      joined.write.csv("max_min_avg_amounts.csv")

    } catch {
      case e: Throwable => println(e)
    } finally {
      Thread.sleep(100000000)
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
      .cache()
  }

  def amountStatistics(amounts: Dataset[Amount]): Dataset[PositionStatistics] = {
    val defaultSparkDecimalDT = DecimalType(38, 18)
    val lastValueByTimestamp = LastValueByTimestamp(defaultSparkDecimalDT)
    amounts
      .groupByKey(_.positionId)
      .agg(
        avg("amount")
          .name("avg")
          .cast(defaultSparkDecimalDT) // have to do custom casting for avg
          .as[java.math.BigDecimal],
        min("amount")
          .name("min")
          .as[java.math.BigDecimal],
        max("amount")
          .name("max")
          .as[java.math.BigDecimal],
        lastValueByTimestamp($"amount", $"eventTime")
          .name("currentAmount")
          .as[java.math.BigDecimal]
      )
      .cache()
  }
}
