package es.upo.datalab.entrypoints.tests

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by davgutavi on 12/07/17.
  */
object Funcion extends MapFunction[Row,Timestamp]{

  def   call(value: Row): Timestamp={

    val sdf = new SimpleDateFormat("YYYYMMddHHmmss")

    val t1 = sdf.parse(value.getString(3))

    new Timestamp(t1.getTime)

//    println("FROM "+value.getString(3)+" , TO "+ts)


  }

}
