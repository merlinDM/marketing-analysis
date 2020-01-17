package com.gd.model

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

class AttributionSource extends DateParser {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read(): DataFrame = {
    spark.createDataFrame(data)
  }

  private val data = Seq(
    AttributionRecord(purchaseId = Some("p1"),	purchaseTime = Some(parseDate("2019-01-01 0:01:05")),	billingCost = Some(100.5), isConfirmed = Some(true), sessionId = Some("u1_1"), campaignId = Some("cmp1"), channelId = Some("Google Ads")),
    AttributionRecord(purchaseId = Some("p2"),	purchaseTime = Some(parseDate("2019-01-01 0:03:10")),	billingCost = Some(200), isConfirmed = Some(true), sessionId = Some("u2_1"), campaignId = Some("cmp1"), channelId = Some("Yandex Ads")),
    AttributionRecord(purchaseId = Some("p3"),	purchaseTime = Some(parseDate("2019-01-01 1:12:15")),	billingCost = Some(300), isConfirmed = Some(false), sessionId = Some("u3_2"), campaignId = Some("cmp1"), channelId = Some("Google Ads")),
    AttributionRecord(purchaseId = Some("p4"),	purchaseTime = Some(parseDate("2019-01-01 2:13:05")),	billingCost = Some(50.2), isConfirmed = Some(true), sessionId = Some("u3_3"), campaignId = Some("cmp2"), channelId = Some("Yandex Ads")),
    AttributionRecord(purchaseId = Some("p5"),	purchaseTime = Some(parseDate("2019-01-01 2:15:05")),	billingCost = Some(75), isConfirmed = Some(true), sessionId = Some("u3_3"), campaignId = Some("cmp2"), channelId = Some("Yandex Ads")),
    AttributionRecord(purchaseId = None,	purchaseTime = None,	billingCost = None, isConfirmed = None, sessionId = Some("u3_1"), campaignId = Some("cmp2"), channelId = Some("Facebook Ads")),
    AttributionRecord(purchaseId = None,	purchaseTime = None,	billingCost = None, isConfirmed = None, sessionId = Some("u2_2"), campaignId = Some("cmp2"), channelId = Some("Yandex Ads"))

  )

}

case class AttributionRecord(purchaseId: Option[String],
                             purchaseTime: Option[Timestamp],
                             billingCost: Option[Double],
                             isConfirmed: Option[Boolean],
                             sessionId: Option[String],
                             campaignId: Option[String],
                             channelId: Option[String]
                            )
