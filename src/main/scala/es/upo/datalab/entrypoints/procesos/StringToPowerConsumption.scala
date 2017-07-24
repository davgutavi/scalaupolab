package es.upo.datalab.entrypoints.procesos

import java.util.regex.Pattern

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

object StringToPowerConsumption extends MapFunction[Row,Row] {

  def call(value: Row): Row = {

    val rconsume = value.getString(9)
    val vconsume = value.getString(10)
    val tconsume = value.getString(16)


    val vr = rconsume.split("\\$")
    val vs = vconsume.split("\\$")
    val vt = tconsume.split("\\$")


    val v = vs.map(l => {

      val  w1 = l.split(("\\|"))

      if(w1.length!=2){
          "-1".toInt
      }else{
        w1(0).toInt
      }
    })



    val vv = vs.map(l => {

      val  w1 = l.split(("\\|"))


        if(w1.length!=2){
         "g"
        }else{
       w1(1).toString
        }


    }).mkString("-")

    val r = vr.map(l => {

      val  w1 = l.split(("\\|"))

      if(w1.length!=2){
        "-1".toInt
      }else{
        w1(0).toInt
      }
    })

    val t = vt.map(l => {

      val  w1 = l.split(("\\|"))

      if(w1.length!=2){
        "-1".toInt
      }else{
        w1(0).toInt
      }
    })

    Row.fromSeq(  v.union(r).union(t))


  }

}
