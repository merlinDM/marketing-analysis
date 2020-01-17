package com.gd

import org.apache.spark.sql.SparkSession

class SetupNode {

  def init(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("marketing-attribution")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.local.dir", "/tmp/spark")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("""CREATE TABLE IF NOT EXISTS default.event (
                |   userId STRING,
                |   eventId STRING,
                |   eventTime TIMESTAMP,
                |   eventType STRING,
                |   attributes MAP <STRING, STRING>
                |  )
                |STORED AS PARQUET
    """.stripMargin)

    spark.sql(
      """CREATE TABLE IF NOT EXISTS default.purchase (
        |    purchaseId STRING,
        |    purchaseTime TIMESTAMP,
        |    billingCost DOUBLE,
        |    isConfirmed BOOLEAN
        |  )
        |STORED AS PARQUET""".stripMargin
    )

    spark.sql(
      """CREATE TABLE IF NOT EXISTS default.attribution (
        |    purchaseId STRING,
        |    purchaseTime TIMESTAMP,
        |    billingCost DOUBLE,
        |    isConfirmed BOOLEAN,
        |    sessionId STRING,
        |    campaignId STRING,
        |    channelId STRING
        |  )
        |STORED AS PARQUET""".stripMargin
    )

    spark.sql(
      """CREATE TABLE IF NOT EXISTS default.top_campaign (
        |     campaignId STRING, revenue DOUBLE
        |   )
        |STORED AS PARQUET
        |""".stripMargin
    )

    spark.sql(
      """CREATE TABLE IF NOT EXISTS default.engagement (
        |     campaignId STRING, channelId STRING
        |   )
        |STORED AS PARQUET
        |""".stripMargin
    )
  }
}
