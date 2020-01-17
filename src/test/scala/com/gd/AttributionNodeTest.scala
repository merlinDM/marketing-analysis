package com.gd

import com.gd.model.{AttributionSource, EventSource, PurchaseSource}
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

  test("test Join method") {

    val eventSource = new EventSource()
    val events = eventSource.read()

    val purchaseSource = new PurchaseSource()
    val purchases = purchaseSource.read()

    val attributionNode = new AttributionNode()
    val attributions = attributionNode.join(events, purchases)

    val attributionSource = new AttributionSource()
    val expectedAttributions = attributionSource.read()

    assert(attributions.schema.equals(expectedAttributions.schema),
      "Resulted dataset should have the same schema as expected one.")

    assert(attributions.exceptAll(expectedAttributions).isEmpty,
      "Resulted dataset should be equivalent to the expected one.")
  }
}
