package com.gd
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}

import scala.collection.mutable

class AttributionNodeAgg(cfg: AttributionNodeConfiguration) extends JoinNode {
  override def join(events: DataFrame, purchases: DataFrame): DataFrame = {
    import events.sparkSession.implicits._

    val aggregator = new SessionAggregator

    val starts: DataFrame = events.as[EventRecord]
      .groupByKey(r => r.userId)
      .agg(aggregator.toColumn)
      .flatMap(_._2)
      .toDF()
      .withColumn("sessionId", concat($"userId", lit("_"), $"sessionId"))
      .drop("userId")

    val attributions = purchases.join(starts, Seq("purchaseId"), "full")

    attributions
  }

}

sealed case class SessionRecord(
  userId: String = "",
  sessionId: Integer = 0,
  campaignId: String = "",
  channelId: String = "",
  purchaseId: Option[String] = None
)

class SessionAggregator extends Aggregator[EventRecord, Seq[EventRecord], Seq[SessionRecord]] {
  override def zero: Seq[EventRecord] = mutable.Seq.empty[EventRecord]

  override def reduce(b: Seq[EventRecord], a: EventRecord): Seq[EventRecord] = {
    if (a.eventType == "purchase" || a.eventType == "app_open")
      b :+ a
    else
      b
  }

  override def merge(b1: Seq[EventRecord], b2: Seq[EventRecord]): Seq[EventRecord] = b1 ++ b2

  override def finish(reduction: Seq[EventRecord]): Seq[SessionRecord] = {

    val zeroValue = mutable.Seq.empty[SessionRecord]

    reduction
      .sortBy(r => r.eventTime.getTime)
      .foldLeft[Seq[SessionRecord]](zeroValue) {
        case (records: mutable.Seq[SessionRecord], record: EventRecord) =>

          var session = SessionRecord(userId = record.userId)
          var result = zeroValue

          if (records.nonEmpty) {
            session = records.head
            result = records.tail
          }

          if (record.eventType == "app_open") {
            result = records
            session = SessionRecord(userId = record.userId, sessionId = session.sessionId + 1)

            record.attributes.foreach(m => {
              m.get("channelId")
                .foreach(channel => session = session.copy(channelId = channel))
              m.get("campaignId")
                .foreach(campaign => session = session.copy(campaignId = campaign))
            })
          }

          if (record.eventType == "purchase") {

            if (session.purchaseId.isDefined) {
              result = result.+:(session)
              session = session.copy(purchaseId = None)
            }

            record.attributes.foreach(m => {
              m.get("purchaseId")
                .foreach(purchase => session = session.copy(purchaseId = Some(purchase)))
            })
          }

          result.+:(session)
      }
  }

  override def bufferEncoder: Encoder[Seq[EventRecord]] = Encoders.kryo

  override def outputEncoder: Encoder[Seq[SessionRecord]] = Encoders.kryo
}