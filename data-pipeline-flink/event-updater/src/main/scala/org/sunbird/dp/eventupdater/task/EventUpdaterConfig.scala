package org.sunbird.dp.eventupdater.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.dp.core.job.BaseJobConfig
import org.apache.flink.streaming.api.scala._

class EventUpdaterConfig(override val config: Config) extends BaseJobConfig(config, jobName = "EventUpdaterJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val jobParallelism: Int = config.getInt("task.job.parallelism")

  // PostgreSQL Configuration
  val postgresUser: String = "postgres"
  val postgresPassword: String = "password123"
  val postgresTable: String = "user_detail"
  val postgresDb: String = "test_warehouse"
  val postgresHost: String = "10.175.3.37"
  val postgresPort: Int = 5432
  val postgresMaxConnections: Int = 2

  // Metrics
  val successCount = "success-event-count"
  val failedEventCount = "failed-event-count"
}