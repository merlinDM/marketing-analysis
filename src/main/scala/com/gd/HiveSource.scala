package com.gd

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class HiveSource(cfg: HiveSourceConfiguration = HiveSourceConfiguration()) {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read(): DataFrame = {

    spark.table(cfg.input)

  }

  def write(df: DataFrame): Unit = {

    // Throws exception if schemas doesn't match and ensures correct column ordering.
    df.write
      .mode(cfg.mode)
      .format("hive")
      .saveAsTable(cfg.output)

  }

}

case class HiveSourceConfiguration(input: String = "default.test_input",
                                   output: String = "default.test_output",
                                   mode: SaveMode = SaveMode.Append)