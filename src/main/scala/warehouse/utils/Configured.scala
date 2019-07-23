package warehouse.utils

import com.typesafe.config.{Config, ConfigFactory}

trait Configured {
  lazy val config: Config = ConfigFactory.load()
}
