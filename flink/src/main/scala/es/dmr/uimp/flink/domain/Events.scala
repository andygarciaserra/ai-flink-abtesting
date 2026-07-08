package es.dmr.uimp.flink.domain

/** Message read from topic_demographic. */
case class DemographicEvent(
  uuid: Long,
  age: Double,
  man: Int,
  woman: Int
)

/** Message read from topic_historic. */
case class HistoricEvent(
  uuid: Long,
  products: Map[String, Int]
)

/** Internal object after joining demographic and historic information by uuid. */
case class JoinedCustomer(
  uuid: Long,
  age: Double,
  man: Int,
  woman: Int,
  products: Map[String, Int]
)

/** Message written to topic_student_prediction. */
case class PredictionEvent(
  uuid: Long,
  value: Int,
  token: String
)
