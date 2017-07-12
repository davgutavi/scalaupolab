package es.upo.datalab.utilities

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.Row

/**
  * Created by davgutavi on 12/07/17.
  */
object FuntionUtils {



  def timeStamp (r:Row):Timestamp={

    val date = r.getDate(9)
    val time = r.getTimestamp(10)


    val calendar = Calendar.getInstance()

    calendar.setTime(date)


    var t = calendar.getTimeInMillis() + time.getTime()

    val  n = new Timestamp(t)

    n
//    Row(n)

  }

}
