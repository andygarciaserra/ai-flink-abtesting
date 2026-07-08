package es.dmr.uimp.flink

import es.dmr.uimp.flink.domain.{DemographicEvent, HistoricEvent}
import es.dmr.uimp.flink.join.CustomerJoinFunction
import es.dmr.uimp.flink.json.JsonUtils
import es.dmr.uimp.flink.prediction.PredictionMapFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._

object StudentPredictionJob {

  private val DefaultBrokers =
    "bigdatamaster.dataspartan.com:19093," +
      "bigdatamaster.dataspartan.com:29093," +
      "bigdatamaster.dataspartan.com:39093"

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val brokers = params.get("bootstrap.servers", DefaultBrokers)
    val topicDemographic = params.get("topic.demographic", "topic_demographic")
    val topicHistoric = params.get("topic.historic", "topic_historic")
    val topicPrediction = params.get("topic.prediction", "topic_student_prediction")
    val groupId = params.get("group.id", "uimp-flink-abtesting-student")
    val token = params.get("token", "CHANGE_ME")
    val threshold = params.getDouble("threshold", 0.60)
    val localTest = params.getBoolean("local.test", false)

    require(token != "CHANGE_ME" || localTest, "You must pass --token YOUR_EXPERIMENT_TOKEN when running against Kafka")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    // Checkpointing is useful for stateful joins and Kafka delivery guarantees.
    // It is disabled in local test mode to keep the first smoke test simple.
    if (!localTest) {
      env.enableCheckpointing(params.getLong("checkpoint.ms", 60000L))
    }

    val demographicStream: DataStream[DemographicEvent] =
      if (localTest) buildLocalDemographicStream(env)
      else buildKafkaStringSource(env, brokers, topicDemographic, groupId + "-demographic")
        .flatMap[DemographicEvent]((raw: String) => parseDemographicSafely(raw).toList)
        .name("parse-topic-demographic")

    val historicStream: DataStream[HistoricEvent] =
      if (localTest) buildLocalHistoricStream(env)
      else buildKafkaStringSource(env, brokers, topicHistoric, groupId + "-historic")
        .flatMap[HistoricEvent]((raw: String) => parseHistoricSafely(raw).toList)
        .name("parse-topic-historic")

    val predictions: DataStream[String] = demographicStream
      .keyBy(_.uuid)
      .connect(historicStream.keyBy(_.uuid))
      .process(new CustomerJoinFunction)
      .name("join-by-uuid")
      .map(new PredictionMapFunction(token, threshold))
      .name("predict-with-pmml")

    if (localTest) {
      predictions.print().name("print-local-predictions")
    } else {
      val sink = KafkaSink.builder[String]()
        .setBootstrapServers(brokers)
        .setRecordSerializer(
          KafkaRecordSerializationSchema.builder[String]()
            .setTopic(topicPrediction)
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
        )
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()

      predictions.sinkTo(sink).name("write-topic-student-prediction")
    }

    env.execute("Silocompro student prediction Flink job")
  }

  private def buildKafkaStringSource(
    env: StreamExecutionEnvironment,
    brokers: String,
    topic: String,
    groupId: String
  ): DataStream[String] = {
    val source = KafkaSource.builder[String]()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks[String](), s"source-$topic")
  }

  private def parseDemographicSafely(raw: String): Option[DemographicEvent] = {
    try {
      Some(JsonUtils.parseDemographic(raw))
    } catch {
      case ex: Exception =>
        System.err.println(s"Could not parse demographic message: $raw. Error: ${ex.getMessage}")
        None
    }
  }

  private def parseHistoricSafely(raw: String): Option[HistoricEvent] = {
    try {
      Some(JsonUtils.parseHistoric(raw))
    } catch {
      case ex: Exception =>
        System.err.println(s"Could not parse historic message: $raw. Error: ${ex.getMessage}")
        None
    }
  }

  private def buildLocalDemographicStream(env: StreamExecutionEnvironment): DataStream[DemographicEvent] = {
    env.fromElements(
      DemographicEvent(uuid = 404180L, age = 41.0, man = 1, woman = 0),
      DemographicEvent(uuid = 1234567L, age = 23.0, man = 0, woman = 1)
    )
  }

  private def buildLocalHistoricStream(env: StreamExecutionEnvironment): DataStream[HistoricEvent] = {
    env.fromElements(
      HistoricEvent(uuid = 404180L, products = Map("cat46" -> 1, "cat23" -> 1, "cat34" -> 1, "cat22" -> 1)),
      HistoricEvent(uuid = 1234567L, products = Map("cat17" -> 0, "cat74" -> 0, "cat13" -> 1))
    )
  }
}
