package com.mpojeda84.mapr.scala.helper

import com.mpojeda84.mapr.scala.model.CarDataInstant

object Helper {

  def toJsonWithId(csvLine: String): CarDataInstant = {
    val values = csvLine.split(",").map(_.trim)

    val id = values(0) + values(4) + values(5);

    CarDataInstant(
      id,
      values(0),
      values(1),
      values(2),
      values(3),
      values(4),
      values(5),
      values(6),
      values(7),
      values(8),
      values(9),
      values(10),
      values(11),
      values(12),
      values(13),
      values(14),
      values(15),
      values(16)
    )

  }

}
