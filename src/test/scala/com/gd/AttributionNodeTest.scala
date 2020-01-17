package com.gd

import com.gd.model.{AttributionSource, EventSource, PurchaseSource}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class AttributionNodeTest extends FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("marketing-attribution-test")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.local.dir", "/tmp/spark")
    .getOrCreate()

  test("data samples") {
    val eventSource = new EventSource()
    val events = eventSource.read()

    events.withColumn("campaignId", col("attributes.campainId")).show(100, truncate = false)

    val purchaseSource = new PurchaseSource()
    val purchases = purchaseSource.read()

    purchases.show(100, truncate = false)

    val attributionSource = new AttributionSource()
    val attributions = attributionSource.read()

    attributions.show(100, truncate = false)
  }

  test("test Join method") {

  }
}
