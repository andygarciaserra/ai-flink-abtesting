package es.dmr.uimp.flink.prediction

import es.dmr.uimp.flink.domain.{JoinedCustomer, PredictionEvent}
import es.dmr.uimp.flink.json.JsonUtils
import es.dmr.uimp.flink.model.PmmlLogisticRegression
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import scala.io.Source

/** Loads the PMML model once per Flink task and converts joined customer records into prediction JSON. */
class PredictionMapFunction(token: String, threshold: Double) extends RichMapFunction[JoinedCustomer, String] {

  @transient private var model: PmmlLogisticRegression = _
  @transient private var featureOrder: Seq[String] = _

  override def open(parameters: Configuration): Unit = {
    val classLoader = getClass.getClassLoader

    val pmmlStream = classLoader.getResourceAsStream("model.pmml")
    model = PmmlLogisticRegression.load(pmmlStream)
    if (pmmlStream != null) pmmlStream.close()

    val featureStream = classLoader.getResourceAsStream("feature_order.json")
    require(featureStream != null, "feature_order.json was not found in src/main/resources")
    try {
      val rawFeatureOrder = Source.fromInputStream(featureStream, "UTF-8").mkString
      featureOrder = JsonUtils.parseFeatureOrder(rawFeatureOrder)
    } finally {
      featureStream.close()
    }
  }

  override def map(customer: JoinedCustomer): String = {
    val features = FeatureBuilder.build(customer, featureOrder)
    val probability = model.predictProbability(features)
    val value = if (probability >= threshold) 1 else 0

    JsonUtils.toJson(
      PredictionEvent(
        uuid = customer.uuid,
        value = value,
        token = token
      )
    )
  }
}
