package es.dmr.uimp.flink.join

import es.dmr.uimp.flink.domain.{DemographicEvent, HistoricEvent, JoinedCustomer}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

/**
 * Joins both Kafka input streams by uuid.
 *
 * One topic gives age/sex and the other gives historic products. Since they may arrive in any order,
 * we keep whichever message arrives first in Flink state. When the matching message arrives, we emit
 * the complete JoinedCustomer.
 */
class CustomerJoinFunction extends KeyedCoProcessFunction[Long, DemographicEvent, HistoricEvent, JoinedCustomer] {

  private var demographicState: ValueState[DemographicEvent] = _
  private var historicState: ValueState[HistoricEvent] = _

  override def open(parameters: Configuration): Unit = {
    demographicState = getRuntimeContext.getState(
      new ValueStateDescriptor[DemographicEvent]("demographic-state", classOf[DemographicEvent])
    )

    historicState = getRuntimeContext.getState(
      new ValueStateDescriptor[HistoricEvent]("historic-state", classOf[HistoricEvent])
    )
  }

  override def processElement1(
    demographic: DemographicEvent,
    context: KeyedCoProcessFunction[Long, DemographicEvent, HistoricEvent, JoinedCustomer]#Context,
    out: Collector[JoinedCustomer]
  ): Unit = {
    val historic = historicState.value()

    if (historic != null) {
      out.collect(
        JoinedCustomer(
          uuid = demographic.uuid,
          age = demographic.age,
          man = demographic.man,
          woman = demographic.woman,
          products = historic.products
        )
      )
      historicState.clear()
    } else {
      demographicState.update(demographic)
    }
  }

  override def processElement2(
    historic: HistoricEvent,
    context: KeyedCoProcessFunction[Long, DemographicEvent, HistoricEvent, JoinedCustomer]#Context,
    out: Collector[JoinedCustomer]
  ): Unit = {
    val demographic = demographicState.value()

    if (demographic != null) {
      out.collect(
        JoinedCustomer(
          uuid = historic.uuid,
          age = demographic.age,
          man = demographic.man,
          woman = demographic.woman,
          products = historic.products
        )
      )
      demographicState.clear()
    } else {
      historicState.update(historic)
    }
  }
}
