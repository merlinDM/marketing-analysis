package com.gd.model

import java.sql.Timestamp

import com.gd.PurchaseRecord
import org.apache.spark.sql.{DataFrame, SparkSession}

class PurchaseSource extends DateParser {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read(): DataFrame = {
    spark.createDataFrame(data)
  }

  val data = Seq(
    PurchaseRecord(purchaseId = "p1",	purchaseTime = parseDate("2019-01-01 0:01:05"),	billingCost = 100.5, isConfirmed = true),
    PurchaseRecord(purchaseId = "p2",	purchaseTime = parseDate("2019-01-01 0:03:10"),	billingCost = 200, isConfirmed = true),
    PurchaseRecord(purchaseId = "p3",	purchaseTime = parseDate("2019-01-01 1:12:15"),	billingCost = 300, isConfirmed = false),
    PurchaseRecord(purchaseId = "p4",	purchaseTime = parseDate("2019-01-01 2:13:05"),	billingCost = 50.2, isConfirmed = true),
    PurchaseRecord(purchaseId = "p5",	purchaseTime = parseDate("2019-01-01 2:15:05"),	billingCost = 75, isConfirmed = true)
  )

}