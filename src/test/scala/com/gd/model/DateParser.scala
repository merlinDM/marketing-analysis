package com.gd.model

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

trait DateParser {

  protected def parseDate(s: String): Timestamp = {
    val format = new SimpleDateFormat("yyyy-MM-dd H:mm:ss")
    val date: Date = format.parse(s)

    new Timestamp(date.getTime)
  }

}
