package warehouse.spark

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructField, StructType, TimestampType}


/**
  * NB: although in general case this function is not deterministic, it produces deterministic results
  * if all timestamps differ (or repeated timestamps correspond to exactly the same values)
  */
case class LastValueByTimestamp(valueDT: DataType) extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Seq(
    StructField("value", valueDT),
    StructField("ts", TimestampType)
  ))

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = valueDT

  override def deterministic: Boolean = false

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = new Timestamp(0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val bufferTS = buffer(1).asInstanceOf[Timestamp]
    val inputTS = input(1).asInstanceOf[Timestamp]

    if (bufferTS.before(inputTS)) {
      buffer(0) = input(0)
      buffer(1) = input(1)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Any = buffer(0)
}
