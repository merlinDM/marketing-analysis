package com.gd.model

import org.apache.spark.sql.{DataFrame, SparkSession}

class RevenueTopSource extends DateParser {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read(): DataFrame = {
    spark.createDataFrame(data)
  }

  private val data = Seq(
    RevenueTopRecord(campaignId = "cmp1", revenue = Some(300.5)),
    RevenueTopRecord(campaignId = "cmp2", revenue = Some(125.2))
  )

}

case class RevenueTopRecord(campaignId: String, revenue: Option[Double])