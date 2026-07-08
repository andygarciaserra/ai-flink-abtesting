package es.dmr.uimp.flink.model

import java.io.InputStream
import scala.xml.XML

/**
 * Minimal PMML importer for the logistic-regression PMML generated in the offline stage.
 *
 * It reads the intercept and NumericPredictor coefficients from model.pmml and computes:
 *
 *   probability = sigmoid(intercept + sum(coef_i * feature_i))
 *
 * This keeps the online Flink job simple and makes the PMML dependency explicit.
 */
case class PmmlLogisticRegression(
  intercept: Double,
  coefficients: Map[String, Double]
) extends Serializable {

  def predictProbability(features: Map[String, Double]): Double = {
    val score = coefficients.foldLeft(intercept) {
      case (acc, (featureName, coefficient)) =>
        acc + coefficient * features.getOrElse(featureName, 0.0)
    }

    // Numerically safe sigmoid.
    if (score >= 35.0) 1.0
    else if (score <= -35.0) 0.0
    else 1.0 / (1.0 + math.exp(-score))
  }
}

object PmmlLogisticRegression {
  def load(inputStream: InputStream): PmmlLogisticRegression = {
    require(inputStream != null, "model.pmml was not found in src/main/resources")

    val pmml = XML.load(inputStream)
    val regressionTable = (pmml \\ "RegressionTable")
      .find(node => (node \ "@targetCategory").text == "1")
      .getOrElse((pmml \\ "RegressionTable").head)

    val intercept = (regressionTable \ "@intercept").text.toDouble

    val coefficients = (regressionTable \\ "NumericPredictor")
      .map { node =>
        val name = (node \ "@name").text
        val coefficient = (node \ "@coefficient").text.toDouble
        name -> coefficient
      }
      .toMap

    PmmlLogisticRegression(intercept, coefficients)
  }
}
