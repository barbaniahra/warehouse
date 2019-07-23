package warehouse

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import warehouse.models.{Amount, Position}
import warehouse.spark.LastValueByTimestamp
import warehouse.utils.Configured

import scala.reflect.io.Path

object Warehouse extends Configured {
  val PositionsPath: String = config.getString("warehouse.positions_path")
  val AmountsPath: String = config.getString("warehouse.amounts_path")
  val SaveCurrentAmountsPath: String = config.getString("warehouse.save_current_amounts_path")
  val SaveMaxMinAvgPath: String = config.getString("warehouse.save_max_min_avg_path")

  if (config.getBoolean("warehouse.delete_prev_results")) {
    Seq(SaveCurrentAmountsPath, SaveMaxMinAvgPath).foreach { path =>
      Path(path).deleteRecursively()
    }
  }

  val locatedPositionsPath: String = locateResource(PositionsPath)
  val locatedAmountsPath: String = locateResource(AmountsPath)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Warehouse Statistics")
      .config("spark.master", "local[4]")
      .config("spark.speculation", "true")
      .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    try {
      // Load required data from files, e.g. csv of json.
      val positionsDS: Dataset[Position] = readCsv(locatedPositionsPath, Position.schema, Position.apply)
      val amountsDS: Dataset[Amount] = readCsv(locatedAmountsPath, Amount.schema, Amount.apply)

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

      // saving
      result1.write.csv(SaveCurrentAmountsPath)
      result2.write.csv(SaveMaxMinAvgPath)

//      // showing
//      result1.show()
//      result2.show()
//
//      // detailed
//      println(s"Result1: ${result1.rdd.toDebugString}")
//      println(s"Result2: ${result2.rdd.toDebugString}")

    } catch {
      case any: Throwable =>
        any.printStackTrace()
    } finally {
      println("Press ENTER to close the app")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def locateResource(path: String): String = getClass.getClassLoader.getResource(path).getPath

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
