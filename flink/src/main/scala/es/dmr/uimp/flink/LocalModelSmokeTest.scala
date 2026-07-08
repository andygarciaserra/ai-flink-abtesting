package es.dmr.uimp.flink

import es.dmr.uimp.flink.domain.{JoinedCustomer, PredictionEvent}
import es.dmr.uimp.flink.json.JsonUtils
import es.dmr.uimp.flink.model.PmmlLogisticRegression
import es.dmr.uimp.flink.prediction.FeatureBuilder

import scala.io.Source

/**
 * Tiny smoke test without Flink and without Kafka.
 *
 * It only checks that model.pmml and feature_order.json are readable and that a prediction can be produced.
 */
object LocalModelSmokeTest {
  def main(args: Array[String]): Unit = {
    val threshold = 0.60
    val token = "LOCAL_TEST_TOKEN"

    val classLoader = getClass.getClassLoader

    val pmmlStream = classLoader.getResourceAsStream("model.pmml")
    val model = PmmlLogisticRegression.load(pmmlStream)
    if (pmmlStream != null) pmmlStream.close()

    val featureStream = classLoader.getResourceAsStream("feature_order.json")
    require(featureStream != null, "feature_order.json was not found in src/main/resources")
    val featureOrder = try {
      JsonUtils.parseFeatureOrder(Source.fromInputStream(featureStream, "UTF-8").mkString)
    } finally {
      featureStream.close()
    }

    val customer = JoinedCustomer(
      uuid = 404180L,
      age = 41.0,
      man = 1,
      woman = 0,
      products = Map("cat46" -> 1, "cat23" -> 1, "cat34" -> 1, "cat22" -> 1)
    )

    val features = FeatureBuilder.build(customer, featureOrder)
    val probability = model.predictProbability(features)
    val value = if (probability >= threshold) 1 else 0
    val predictionJson = JsonUtils.toJson(PredictionEvent(customer.uuid, value, token))

    println(f"Probability: $probability%.4f")
    println(s"Threshold:   $threshold")
    println(s"Prediction:  $predictionJson")
  }
}
