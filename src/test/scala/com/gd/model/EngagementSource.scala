package com.gd.model

import com.gd.EngagementRecord

class EngagementSource extends SampleSource[EngagementRecord] with DateParser {

  val data = Seq(
    EngagementRecord(campaignId = "cmp1", channelId = "Google Ads"),
    EngagementRecord(campaignId = "cmp2", channelId = "Yandex Ads")
  )

}