package com.gd
import org.apache.spark.sql.DataFrame

class AttributionNodeAgg(cfg: AttributionNodeConfiguration) extends JoinNode {
  override def join(left: DataFrame, right: DataFrame): DataFrame = ???
}
