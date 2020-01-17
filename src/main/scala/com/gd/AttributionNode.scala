package com.gd

import org.apache.spark.sql.DataFrame

class AttributionNode(cfg: AttributionNodeConfiguration = AttributionNodeConfiguration()) {

  def join(events: DataFrame, purchases: DataFrame): DataFrame = {
    val sessions = retrieveSessions(events)

    sessions.join(purchases, Seq("purchaseId"), "inner")
  }

  private def retrieveSessions(events: DataFrame): DataFrame = ???

}

case class AttributionNodeConfiguration()
