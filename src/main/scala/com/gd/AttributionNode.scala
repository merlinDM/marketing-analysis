package com.gd

import org.apache.spark.sql.DataFrame

class AttributionNode(cfg: AttributionNodeConfiguration = AttributionNodeConfiguration()) extends JoinNode {

  def join(events: DataFrame, purchases: DataFrame): DataFrame = {
    val node = cfg.implementation match {
      case AttributionNodeSQLImpl =>
        new AttributionNodeSQL(cfg)
      case AttributionNodeAggImpl =>
        new AttributionNodeAgg(cfg)
    }

    node.join(events, purchases)
  }

}

case class AttributionNodeConfiguration(
  timeoutSeconds: Integer = Integer.MAX_VALUE,
  implementation: AttributionNodeImpl = AttributionNodeAggImpl
)

sealed trait AttributionNodeImpl
case object AttributionNodeSQLImpl extends AttributionNodeImpl
case object AttributionNodeAggImpl extends AttributionNodeImpl

trait JoinNode {
  def join(left: DataFrame, right: DataFrame): DataFrame
}