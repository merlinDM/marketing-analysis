package com.gd

import com.gd.model.{AttributionSource, RevenueTopRecord, RevenueTopSource, EventSource, PurchaseSource}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class AttributionNodeTest extends FunSuite {

  SparkSession
    .builder()
    .appName("marketing-attribution-test")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.local.dir", "/tmp/spark")
    .getOrCreate()

  test("test samples") {
    val eventSource = new EventSource()
    val events = eventSource.read()

    events.show(100, truncate = false)

    val purchaseSource = new PurchaseSource()
    val purchases = purchaseSource.read()

    purchases.show(100, truncate = false)

    val attributionSource = new AttributionSource()
    val attributions = attributionSource.read()

    attributions.show(100, truncate = false)
  }

  test("test AttributionNode.join()") {

    val eventSource = new EventSource()
    val events = eventSource.read()

    val purchaseSource = new PurchaseSource()
    val purchases = purchaseSource.read()

    val attributionNode = new AttributionNode()
    val attributions = attributionNode.join(events, purchases)

    val attributionSource = new AttributionSource()
    val expectedAttributions = attributionSource.read()

    val diff = attributions.exceptAll(expectedAttributions)
//    diff.sort("purchaseId").show(truncate = false)

//    attributions.show()
//    attributions.printSchema()
//    expectedAttributions.printSchema()

    assert(attributions.schema.equals(expectedAttributions.schema),
      "Resulted dataset should have the same schema as expected one.")

    assert(diff.isEmpty,
      "Resulted dataset should be equivalent to the expected one.")
  }

  test("test InsightQuery.topCampaigns()") {
    val attributionSource = new AttributionSource()
    val attributions = attributionSource.read()

    val query = new InsightQuery()
    val topByRevenue = query.topCampaignsByRevenue(attributions)

    val revenueTopSource = new RevenueTopSource()
    val expectedTop = revenueTopSource.read()

    val diff = topByRevenue.exceptAll(expectedTop)

    assert(topByRevenue.schema.equals(expectedTop),
      "Resulted dataset should have the same schema as expected one.")

    assert(diff.isEmpty,
      "Resulted dataset should be equivalent to the expected one.")
  }

  test("test InsightQuery.channelEngagement()") {
    val attributionSource = new AttributionSource()
    val attributions = attributionSource.read()

    val query = new InsightQuery()
    val engagement = query.channelEngagement(attributions)

    val engagementSource = new RevenueTopSource()
    val expectedEngagement = engagementSource.read()

    val diff = expectedEngagement.exceptAll(engagement)

    assert(engagement.schema.equals(expectedEngagement),
      "Resulted dataset should have the same schema as expected one.")

    assert(diff.isEmpty,
      "Resulted dataset should be equivalent to the expected one.")
  }
}
