package warehouse

import java.io.{BufferedOutputStream, FileOutputStream, PrintWriter}
import java.sql.Timestamp

import warehouse.models.{Amount, Position}
import warehouse.utils.Configured

import scala.math.BigDecimal.RoundingMode


object DataGenerator extends App with Configured {
  val r = new scala.util.Random(seed = 42)

  val PositionsCount = config.getInt("data_generator.positions_count")
  val AmountsCount = config.getInt("data_generator.amounts_count")
  val WarehousesCount = config.getInt("data_generator.warehouses_count")
  val ProductsCount = config.getInt("data_generator.products_count")
  val SaveAmountsPath = config.getString("data_generator.save_amounts_path")
  val SavePositionsPath = config.getString("data_generator.save_positions_path")

  Seq(SaveAmountsPath, SavePositionsPath).foreach { pathToFile =>
    val dir = new java.io.File(pathToFile).getParentFile
    if (!dir.exists())
      dir.mkdir()
  }

  val warehouses = Range(0, WarehousesCount).map(i => s"W-$i")
  val products = Range(0, ProductsCount).map(i => s"P-$i")
  val positions = Range(0, PositionsCount).map { i =>
    Position(
      positionId = i,
      warehouse = warehouses(r.nextInt(warehouses.size)),
      product = products(r.nextInt(products.size)),
      eventTime = new Timestamp(r.nextInt().abs)
    )
  }
  // it's not a good idea to hold 1e8 elements in heap, so let's use stream ;)
  val amounts = Stream.range(0, AmountsCount).view.take(AmountsCount).map { i =>
    Amount(
      positionId = positions(r.nextInt(positions.size)).positionId,
      amount = java.math.BigDecimal.valueOf(r.nextFloat() * r.nextInt()).abs(),
      eventTime = new Timestamp(r.nextInt().abs)
    )
  }

  println("Saving positions")
  withWriter(SavePositionsPath) { writer =>
    positions.foreach { case Position(positionId, warehouse, product, eventTime) =>
      writer.println(s"$positionId, $warehouse, $product, ${eventTime.toInstant.getEpochSecond}")
    }
  }

  println("Saving amounts")
  withWriter(SaveAmountsPath) { writer =>
    amounts.foreach { case Amount(positionId, amount, eventTime) =>
      writer.println(s"$positionId, ${amount.setScale(2, RoundingMode.UP)}, ${eventTime.toInstant.getEpochSecond}")
    }
  }

  def withWriter(path: String)(f: PrintWriter => Unit): Unit = {
    val writer = new PrintWriter(new BufferedOutputStream(new FileOutputStream(path), 50 * 1024 * 1024 /* 50mb */))
    try {
      f(writer)
    } finally {
      writer.close()
    }
  }
}
