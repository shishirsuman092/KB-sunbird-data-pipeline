package org.sunbird.dp.eventupdater.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.dp.eventupdater.functions.EventUpdaterProcessFunction

class EventUpdaterStreamTask(config: EventUpdaterConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {
    //implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic), "KafkaSource")
      .uid("KafkaSource").rebalance()
      .process(new EventUpdaterProcessFunction(config))
      .name("EventUpdaterProcessFunction").uid("EventUpdaterProcessFunction")
      .setParallelism(config.jobParallelism)
    env.execute("Event Updater Job")
  }
}

object EventUpdaterStreamTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("event-updater.conf").withFallback(ConfigFactory.systemEnvironment()))
    val eventUpdaterConfig = new EventUpdaterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(eventUpdaterConfig)
    val task = new EventUpdaterStreamTask(eventUpdaterConfig, kafkaUtil)
    task.process()
  }
}