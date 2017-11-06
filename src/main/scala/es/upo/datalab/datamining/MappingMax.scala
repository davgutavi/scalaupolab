package es.upo.datalab.entrypoints.datasets

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

import scala.collection.mutable

object MappingMax extends MapFunction[Row, Row] {


  def call(r: Row): Row = {

    val l = mutable.MutableList[Any]()




    //Añadiendo campos originales
    original_fields_01(l, r)

    //Añadiendo división por el máximo
    var max:Double = 0.0
    for (i <- 4 to r.length-2) {

      val v = r.getDouble(i)

      if (r.getDouble(i) > max){

        max = v

      }
    }

    if (max==0.0){
      println("MAX a cero ="+max)

    }

    for (j <- 4 to r.length-2) {

      val v = r.getDouble(j)

      val t = v/max


//      println(v+" dividido por "+max+" = "+t)

      l+= t

    }


    l += r.get(r.length-1)


    Row.fromSeq(l)
  }

  def original_fields_01(l: mutable.MutableList[Any], r: Row): Unit = {


    //cpuntmed
    l += r.get(0)

    //ccodpost
    l += r.get(1)

    //cenae
    l += r.get(2)

    //nle
    l += r.get(3)



  }


}
