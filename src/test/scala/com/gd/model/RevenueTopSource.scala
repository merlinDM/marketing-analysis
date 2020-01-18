package com.gd.model

import com.gd.RevenueTopRecord

class RevenueTopSource extends DateParser with SampleSource[RevenueTopRecord] {

  override val data = Seq(
    RevenueTopRecord(campaignId = "cmp1", revenue = Some(300.5)),
    RevenueTopRecord(campaignId = "cmp2", revenue = Some(125.2))
  )

}