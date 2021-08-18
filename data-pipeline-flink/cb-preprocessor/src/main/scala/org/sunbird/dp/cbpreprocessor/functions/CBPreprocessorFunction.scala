package org.sunbird.dp.cbpreprocessor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.cbpreprocessor.domain.Event
import org.sunbird.dp.cbpreprocessor.task.CBPreprocessorConfig

class CBPreprocessorFunction(config: CBPreprocessorConfig,
                             @transient var cbEventsFlattener: CBEventsFlattener = null,
                             @transient var dedupEngine: DedupEngine = null)
                            (implicit val eventTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CBPreprocessorFunction])

  override def metricsList(): List[String] = {
    List(
      config.duplicationSkippedEventMetricsCount,
      config.cbAuditEventRouterMetricCount
    ) ::: deduplicationMetrics
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)
    }
    if (cbEventsFlattener == null) {
      cbEventsFlattener = CBEventsFlattener(config)
    }
  }

  override def close(): Unit = {
    super.close()
    dedupEngine.closeConnectionPool()
  }

  def isDuplicateCheckRequired(producerId: String): Boolean = {
    config.includedProducersForDedup.contains(producerId)
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {

    // node, competency/role/activity/workorder state (Draft, Approved, Published)
    context.output(config.CBEventsOutputTag, event)
    metrics.incCounter(metric = config.CBEventsMetricsCount)

    if (event.hasWorkOrderData() && event.isPublished()) {  // TODO: implement hasWorkOrderData() and isPublished()
        val events = cbEventsFlattener.flatten(event)  // TODO: correct signature
        events.forEach(watEvent => {
          context.output(config.entitiesnodeWorkOrderEventsOutputTag, event)
          metrics.incCounter(metric = config.publishedCBEventsMetricsCount)
        })
    }
  }

}