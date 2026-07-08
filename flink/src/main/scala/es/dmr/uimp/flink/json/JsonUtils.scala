package es.dmr.uimp.flink.json

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import es.dmr.uimp.flink.domain.{DemographicEvent, HistoricEvent, PredictionEvent}

object JsonUtils {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // The statement shows examples with single quotes, while real Kafka messages should be valid JSON.
  // Enabling this keeps the parser robust for both cases.
  mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)

  def parseDemographic(raw: String): DemographicEvent = {
    mapper.readValue(raw, classOf[DemographicEvent])
  }

  def parseHistoric(raw: String): HistoricEvent = {
    mapper.readValue(raw, classOf[HistoricEvent])
  }

  def parseFeatureOrder(raw: String): Seq[String] = {
    mapper.readValue(raw, classOf[Array[String]]).toSeq
  }

  def toJson(prediction: PredictionEvent): String = {
    mapper.writeValueAsString(prediction)
  }
}
