package warehouse

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import warehouse.models.{Amount, Position}
import warehouse.spark.LastValueByTimestamp

import scala.reflect.io.Path

object Warehouse {
  val POSITIONS_PATH = "/positions.csv"
  val AMOUNTS_PATH = "/amounts.csv"

  val SAVE_CURRENT_AMOUNTS_PATH = "current_amounts.csv"
  val SAVE_MAX_MIN_AVG_PATH = "max_min_avg.csv"

  Seq(SAVE_CURRENT_AMOUNTS_PATH, SAVE_MAX_MIN_AVG_PATH).foreach { path =>
    Path(path).deleteRecursively()
  }

  val positionsPath: String = locateResource(POSITIONS_PATH)
  val amountsPath: String = locateResource(AMOUNTS_PATH)

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

      //region Find the current amount for each position, warehouse, product.

      val curAmounts = currentAmounts(amountsDS)

      val positionWithCurrentAmount = positionsDS
        .joinWith(curAmounts,  positionsDS("positionId") === curAmounts("value"))
        .cache()

      val result1 = positionWithCurrentAmount
        .map { case (position, (_, currentAmount)) =>
          (position.positionId, position.warehouse, position.product, currentAmount)
        }
      //endregion

      positionWithCurrentAmount.show()

      //region Find max, min, avg amounts for each warehouse and product

      val result2 = positionWithCurrentAmount
        .groupByKey { case (position, _) => position.warehouse -> position.product }
        .agg(
          avg(positionWithCurrentAmount("_2.currentAmount"))
            .name("avg")
            .cast(DecimalType(38, 18)) // have to do custom casting for avg
            .as[java.math.BigDecimal],
          min(positionWithCurrentAmount("_2.currentAmount"))
            .name("min")
            .as[java.math.BigDecimal],
          max(positionWithCurrentAmount("_2.currentAmount"))
            .name("max")
            .as[java.math.BigDecimal]
        )
        .map { case ((a, b), c, d, e) =>
          (a, b, c, d, e)
        }
      //endregion

      result2.show()

      // saving
      result1.write.csv(SAVE_CURRENT_AMOUNTS_PATH)
      result2.write.csv(SAVE_MAX_MIN_AVG_PATH)

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
      .cache()
  }

  def currentAmounts(amounts: Dataset[Amount]): Dataset[(Long, java.math.BigDecimal)] = {
    val lastValueByTimestamp = LastValueByTimestamp(DecimalType(38, 18))
    amounts
      .groupByKey(_.positionId)
      .agg(
        lastValueByTimestamp($"amount", $"eventTime")
          .name("currentAmount")
          .as[java.math.BigDecimal]
      )
  }
}
