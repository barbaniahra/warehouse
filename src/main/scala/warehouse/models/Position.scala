package warehouse.models

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class Position(positionId: Long,
                    warehouse: String,
                    product: String,
                    eventTime: Timestamp)

object Position {
  lazy val schema: StructType = StructType(Seq(
    StructField("positionId", LongType, nullable = true),
    StructField("warehouse", StringType, nullable = true),
    StructField("product", StringType, nullable = true),
    StructField("eventTime", TimestampType, nullable = true),
  ))

  def apply(row: Row): Position = new Position(
    positionId = row.getAs("positionId"),
    warehouse = row.getAs("warehouse"),
    product = row.getAs("product"),
    eventTime = row.getAs("eventTime")
  )
}
