package com.gd.model

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

class AttributionSource extends DateParser {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read(): DataFrame = {
    spark.createDataFrame(data)
  }

  private val data = Seq(
    AttributionRecord(purchaseId = "p1",	purchaseTime = parseDate("2019-01-01 0:01:05"),	billingCost = 100.5, isConfirmed = true, sessionId = "u1_1", campaignId = "cmp1", channelId = "Google Ads"),
    AttributionRecord(purchaseId = "p2",	purchaseTime = parseDate("2019-01-01 0:03:10"),	billingCost = 200, isConfirmed = true, sessionId = "u2_1", campaignId = "cmp1", channelId = "Yandex Ads"),
    AttributionRecord(purchaseId = "p3",	purchaseTime = parseDate("2019-01-01 1:12:15"),	billingCost = 300, isConfirmed = false, sessionId = "u3_2", campaignId = "cmp1", channelId = "Google Ads"),
    AttributionRecord(purchaseId = "p4",	purchaseTime = parseDate("2019-01-01 2:13:05"),	billingCost = 50.2, isConfirmed = true, sessionId = "u3_3", campaignId = "cmp2", channelId = "Yandex Ads"),
    AttributionRecord(purchaseId = "p5",	purchaseTime = parseDate("2019-01-01 2:15:05"),	billingCost = 75, isConfirmed = true, sessionId = "u3_3", campaignId = "cmp2", channelId = "Yandex Ads")
  )

}

case class AttributionRecord(purchaseId: String,
                             purchaseTime: Timestamp,
                             billingCost: Double,
                             isConfirmed: Boolean,
                             sessionId: String,
                             campaignId: String,
                             channelId: String
                            )
