package com.gd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

class AttributionNode(cfg: AttributionNodeConfiguration = AttributionNodeConfiguration()) {

  def join(events: DataFrame, purchases: DataFrame): DataFrame = {
    val sessions = retrieveSessions(events)

    val recordedPurchases = retrievePurchases(sessions)

    purchases.join(recordedPurchases, Seq("purchaseId"), "full")
  }

  private def retrieveSessions(events: DataFrame): DataFrame = {
    import events.sparkSession.implicits._

    val wnd = Window.partitionBy($"userId").orderBy($"eventTime".asc)

    val sessionIdx = coalesce(
      sum("sessionIdx").over(wnd.rowsBetween(Window.unboundedPreceding, -1)),
      lit(0)
    )

    val newSessionOnTimeoutS = $"eventTime".cast(IntegerType) -
      lag($"eventTime".cast(IntegerType), 1, Integer.MAX_VALUE).over(wnd) > cfg.timeoutSeconds

    val newSessionOnAppOpen = $"eventType".equalTo("app_open")

    val sessionStarted = newSessionOnAppOpen.or(newSessionOnTimeoutS)

    val eventsWithSessions = events
      .withColumn("sessionIdx", sessionStarted.cast(IntegerType))
      .withColumn("sessionIdx", sessionIdx + $"sessionIdx")
      .withColumn("sessionId", concat($"userId", lit("_"), $"sessionIdx".cast(StringType)))
      .drop("sessionIdx")
      .withColumn("campaignId", $"attributes.campaignId")
      .withColumn("channelId", $"attributes.channelId")
      .withColumn("purchaseId", $"attributes.purchaseId")

//    eventsWithSessions
//      .sort("userId", "eventTime")
//      .show(100, truncate = false)

    eventsWithSessions

  }

  private def retrievePurchases(sessions: DataFrame): DataFrame = {
    import sessions.sparkSession.implicits._

    val groupedSessions = sessions.groupBy("sessionId").agg(
      first($"campaignId").as("campaignId"),
      first($"channelId").as("channelId"),
      collect_list($"purchaseId").as("purchases")
    )

    val purchases = groupedSessions
      .withColumn("purchaseId", explode_outer($"purchases"))
      .drop("purchases")

//    purchases
//      .sort("sessionId")
//      .show(100, truncate = false)

    purchases

  }

}

case class AttributionNodeConfiguration(timeoutSeconds: Integer = Integer.MAX_VALUE)
