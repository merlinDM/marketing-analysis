package com.gd.model
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe._

trait SampleSource[T <: Product] {

  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def read()(implicit tag: TypeTag[T]): DataFrame = {
    spark.createDataFrame(data)
  }

  val data: Seq[T]

}
