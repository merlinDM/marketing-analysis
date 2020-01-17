package com.gd

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class InsightQuery {

  private val spark: SparkSession = SparkSession.builder().getOrCreate()

  def topCampaignsByRevenue(attribution: DataFrame): DataFrame = {
    import attribution.sparkSession.implicits._

    val wnd = Window.orderBy($"revenue".desc)

    attribution
      .where("isConfirmed")
      .groupBy("campaignId")
      .agg(sum("billingCost").as("revenue"))
      .withColumn("rnk", row_number().over(wnd))
      .where("rnk < 11")
      .drop("rnk")
  }

  def channelEngagement(attribution: DataFrame): DataFrame = {
    import attribution.sparkSession.implicits._

    val wnd = Window.partitionBy("campaignId").orderBy($"cnt".desc)

    val res = attribution
      .groupBy("campaignId", "channelId")
      .agg(count("*").as("cnt"))
      .withColumn("rnk", row_number().over(wnd))
      .where("rnk = 1")
      .drop("rnk", "cnt")

    res
  }
}
