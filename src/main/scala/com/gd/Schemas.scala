package com.gd

import java.sql.Timestamp

case class EventRecord(userId: String,
  eventId: String,
  eventTime: Timestamp,
  eventType: String,
  attributes: Option[Map[String, String]]
)

case class PurchaseRecord(purchaseId: String,
  purchaseTime: Timestamp,
  billingCost: Double,
  isConfirmed: Boolean
)

case class AttributionRecord(purchaseId: Option[String],
  purchaseTime: Option[Timestamp],
  billingCost: Option[Double],
  isConfirmed: Option[Boolean],
  sessionId: Option[String],
  campaignId: Option[String],
  channelId: Option[String]
)

case class RevenueTopRecord(
  campaignId: String,
  revenue: Option[Double]
)

case class EngagementRecord(
  campaignId: String,
  channelId: String
)
