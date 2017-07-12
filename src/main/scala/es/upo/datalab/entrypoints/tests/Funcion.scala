package es.upo.datalab.entrypoints.tests

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row
import java.sql.{Date, Timestamp}
import java.util.Calendar

/**
  * Created by davgutavi on 12/07/17.
  */
object Funcion extends MapFunction[Row,Timestamp]{

  def   call(value: Row): Timestamp={


    val calendar = Calendar.getInstance()
              calendar.setTime(value.getDate(14))
              new Timestamp(calendar.getTimeInMillis())



  }

}
