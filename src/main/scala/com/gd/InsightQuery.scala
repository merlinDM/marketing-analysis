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
      .withColumn("rnk", dense_rank().over(wnd).as("rnk"))
      .where("rnk < 11")
      .drop("rnk")
  }

  def channelEngagement(attribution: DataFrame): DataFrame = {
    attribution
  }
}
