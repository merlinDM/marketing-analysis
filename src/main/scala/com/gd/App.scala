package com.gd

object App {

  def main(args: Array[String]): Unit = {
    // TODO: Read all configurations from file

    val eventSourceConfig = HiveSourceConfiguration(input = "default.event")
    val eventSource = new HiveSource(eventSourceConfig)
    val events = eventSource.read()

    val purchaseSourceConfig = HiveSourceConfiguration(input = "default.purchase")
    val purchaseSource = new HiveSource(purchaseSourceConfig)
    val purchases = purchaseSource.read()

    val oneHour = 60 * 60
    val attributionNodeConfiguration = AttributionNodeConfiguration(oneHour)
    val attributionNode = new AttributionNode(attributionNodeConfiguration)
    val attributions = attributionNode.join(events, purchases).cache()

    val attributionSinkConfig = HiveSourceConfiguration(output = "default.attribution")
    val attributionSink = new HiveSource(attributionSinkConfig)
    attributionSink.write(attributions)

    val query = new InsightQuery()
    val topCampaigns = query.topCampaignsByRevenue(attributions)
    val engagements = query.channelEngagement(attributions)

    val topCampaignSinkConfig = HiveSourceConfiguration(output = "default.top_campaign")
    val topCampaignSink = new HiveSource(topCampaignSinkConfig)
    topCampaignSink.write(topCampaigns)

    val engagementSinkConfig = HiveSourceConfiguration(output = "default.engagement")
    val engagementSink = new HiveSource(engagementSinkConfig)
    engagementSink.write(engagements)

  }
}
