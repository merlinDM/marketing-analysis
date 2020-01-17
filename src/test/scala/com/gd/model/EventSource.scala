package com.gd.model

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

class EventSource extends DateParser {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read(): DataFrame = {
    spark.createDataFrame(data)
  }

  private val data = Seq(
    EventRecord(userId = "u1", eventId = "u1_e1", eventTime = parseDate("2019-01-01 0:00:00"), eventType = "app_open", attributes = Some(Map("campaignId" -> "cmp1", "channelId" -> "Google Ads"))),
    EventRecord(userId = "u1", eventId = "u1_e2", eventTime = parseDate("2019-01-01 0:00:05"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u1", eventId = "u1_e3", eventTime = parseDate("2019-01-01 0:00:10"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u1", eventId = "u1_e4", eventTime = parseDate("2019-01-01 0:00:15"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u1", eventId = "u1_e5", eventTime = parseDate("2019-01-01 0:00:20"), eventType = "view_product_details", attributes = None),
    EventRecord(userId = "u1", eventId = "u1_e6", eventTime = parseDate("2019-01-01 0:01:00"), eventType = "purchase", attributes = Some(Map("purchaseId" -> "p1"))),
    EventRecord(userId = "u1", eventId = "u1_e7", eventTime = parseDate("2019-01-01 0:02:00"), eventType = "app_close", attributes = None),
    EventRecord(userId = "u2", eventId = "u2_e1", eventTime = parseDate("2019-01-01 0:00:00"), eventType = "app_open", attributes = Some(Map("campaignId" -> "cmp1", "channelId" -> "Yandex Ads"))),
    EventRecord(userId = "u2", eventId = "u2_e2", eventTime = parseDate("2019-01-01 0:00:03"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u2", eventId = "u2_e3", eventTime = parseDate("2019-01-01 0:01:00"), eventType = "view_product_details", attributes = None),
    EventRecord(userId = "u2", eventId = "u2_e4", eventTime = parseDate("2019-01-01 0:03:00"), eventType = "purchase", attributes = Some(Map("purchaseId" -> "p2"))),
    EventRecord(userId = "u2", eventId = "u2_e5", eventTime = parseDate("2019-01-01 0:04:00"), eventType = "app_close", attributes = None),
    EventRecord(userId = "u2", eventId = "u2_e6", eventTime = parseDate("2019-01-02 0:00:00"), eventType = "app_open", attributes = Some(Map("campaignId" -> "cmp2", "channelId" -> "Yandex Ads"))),
    EventRecord(userId = "u2", eventId = "u2_e7", eventTime = parseDate("2019-01-02 0:00:03"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u2", eventId = "u2_e8", eventTime = parseDate("2019-01-02 0:01:00"), eventType = "view_product_details", attributes = None),
    EventRecord(userId = "u2", eventId = "u2_e10", eventTime = parseDate("2019-01-02 0:04:00"), eventType = "app_close", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e1", eventTime = parseDate("2019-01-01 0:00:00"), eventType = "app_open", attributes = Some(Map("campaignId" -> "cmp2", "channelId" -> "Facebook Ads"))),
    EventRecord(userId = "u3", eventId = "u3_e2", eventTime = parseDate("2019-01-01 0:00:10"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e3", eventTime = parseDate("2019-01-01 0:00:30"), eventType = "view_product_details", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e4", eventTime = parseDate("2019-01-01 0:02:00"), eventType = "app_close", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e5", eventTime = parseDate("2019-01-01 1:11:11"), eventType = "app_open", attributes = Some(Map("campaignId" -> "cmp1", "channelId" -> "Google Ads"))),
    EventRecord(userId = "u3", eventId = "u3_e6", eventTime = parseDate("2019-01-01 1:11:20"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e7", eventTime = parseDate("2019-01-01 1:11:40"), eventType = "view_product_details", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e8", eventTime = parseDate("2019-01-01 1:12:00"), eventType = "purchase", attributes = Some(Map("purchaseId" -> "p3"))),
    EventRecord(userId = "u3", eventId = "u3_e9", eventTime = parseDate("2019-01-01 1:12:30"), eventType = "app_close", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e10", eventTime = parseDate("2019-01-02 2:00:00"), eventType = "app_open", attributes = Some(Map("campaignId" -> "cmp2", "channelId" -> "Yandex Ads"))),
    EventRecord(userId = "u3", eventId = "u3_e11", eventTime = parseDate("2019-01-02 2:00:06"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e12", eventTime = parseDate("2019-01-02 2:00:20"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e13", eventTime = parseDate("2019-01-02 2:11:15"), eventType = "view_product_details", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e14", eventTime = parseDate("2019-01-02 2:13:00"), eventType = "purchase", attributes = Some(Map("purchaseId" -> "p4"))),
    EventRecord(userId = "u3", eventId = "u3_e15", eventTime = parseDate("2019-01-02 2:14:00"), eventType = "search_product", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e16", eventTime = parseDate("2019-01-02 2:14:15"), eventType = "view_product_details", attributes = None),
    EventRecord(userId = "u3", eventId = "u3_e17", eventTime = parseDate("2019-01-02 2:15:00"), eventType = "purchase", attributes = Some(Map("purchaseId" -> "p5"))),
    EventRecord(userId = "u3", eventId = "u3_e18", eventTime = parseDate("2019-01-02 2:15:40"), eventType = "app_close", attributes = None)
  )

}

case class EventRecord(userId: String,
                       eventId: String,
                       eventTime: Timestamp,
                       eventType: String,
                       attributes: Option[Map[String, String]])