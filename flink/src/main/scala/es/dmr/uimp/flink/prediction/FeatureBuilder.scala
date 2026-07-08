package es.dmr.uimp.flink.prediction

import es.dmr.uimp.flink.domain.JoinedCustomer

object FeatureBuilder {

  /**
   * Builds the exact feature map expected by the PMML model.
   * Missing product categories are interpreted as 0 purchases.
   */
  def build(customer: JoinedCustomer, featureOrder: Seq[String]): Map[String, Double] = {
    featureOrder.map {
      case "age"   => "age" -> customer.age
      case "man"   => "man" -> customer.man.toDouble
      case "woman" => "woman" -> customer.woman.toDouble
      case productCategory =>
        productCategory -> customer.products.getOrElse(productCategory, 0).toDouble
    }.toMap
  }
}
