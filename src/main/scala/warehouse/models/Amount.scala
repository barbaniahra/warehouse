package warehouse.models

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class Amount(positionId: Long,
                  amount: java.math.BigDecimal,
                  eventTime: Timestamp)

object Amount {
  lazy val schema: StructType = StructType(Seq(
    StructField("positionId", LongType, nullable = true),
    StructField("amount", DecimalType(precision = 38, scale = 10), nullable = true),
    StructField("eventTime", TimestampType, nullable = true),
  ))

  def apply(row: Row): Amount = new Amount(
    positionId = row.getAs("positionId"),
    amount = row.getAs("amount"),
    eventTime = row.getAs("eventTime")
  )
}