package com.gd

import org.apache.spark.sql.{DataFrame, SparkSession}

class InsightQuery {

  private val spark: SparkSession = SparkSession.builder().getOrCreate()

  def topCampaignsByRevenue(projection: DataFrame): DataFrame = ???

  def channelEngagement(projection: DataFrame): DataFrame = ???
}
