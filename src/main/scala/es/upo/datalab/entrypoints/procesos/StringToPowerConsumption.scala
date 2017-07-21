package es.upo.datalab.entrypoints.procesos

import java.util.regex.Pattern

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

object StringToPowerConsumption extends MapFunction[Row,Row] {

  def call(value: Row): Row = {


    val consumos = value.getString(10)


    println("c = "+consumos+"\n")

    val v1 = consumos.split("\\$")

//    println("v1 = "+v1.mkString("-")+"\n")

    val v2 = v1.map(l => {

      val  w1 = l.split(("\\|"))

//      println(l)
//      println(w1.mkString("--"))

      if(w1.length==0){
          "-1".toInt
      }else{
        w1(0).toInt
      }


    })

//    val r = v2.mkString("#")

    Row.fromSeq( v2)

  }

}
