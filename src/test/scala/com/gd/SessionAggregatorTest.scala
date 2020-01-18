package com.gd

import com.gd.model.EventSource
import org.scalatest.{FunSuite, Matchers}

class SessionAggregatorTest extends FunSuite with Matchers {

  test("testFinish") {


    val eventSource = new EventSource()
    val events = eventSource.data
      .filter(r => r.userId == "u3" && (r.eventType == "app_open" || r.eventType == "purchase"))

    val aggregator = new SessionAggregator

    val res = aggregator.finish(events)

    val expected = Seq(
      SessionRecord(userId = "u3",sessionId = 3,campaignId = "cmp2",channelId = "Yandex Ads",purchaseId = Some("p5")),
      SessionRecord(userId = "u3",sessionId = 3,campaignId = "cmp2",channelId = "Yandex Ads",purchaseId = Some("p4")),
      SessionRecord(userId = "u3",sessionId = 2,campaignId = "cmp1",channelId = "Google Ads",purchaseId = Some("p3")),
      SessionRecord(userId = "u3",sessionId = 1,campaignId = "cmp2",channelId = "Facebook Ads",purchaseId = None)
    )

    res should equal (expected)

  }

}
