package com.gd.model

import com.gd.EngagementRecord
import org.apache.spark.sql.{DataFrame, SparkSession}

class EngagementSource extends DateParser {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read(): DataFrame = {
    spark.createDataFrame(data)
  }

  private val data = Seq(
    EngagementRecord(campaignId = "cmp1", channelId = "Google Ads"),
    EngagementRecord(campaignId = "cmp2", channelId = "Yandex Ads")
  )

}