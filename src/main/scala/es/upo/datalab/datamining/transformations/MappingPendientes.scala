package es.upo.datalab.datamining.transformations

import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.math.atan

object MappingPendientes {


  def call(r: Row): Row = {

    val l = mutable.MutableList[Any]()

    //Añadiendo campos originales
    original_fields_01(l, r)





    for (i <- 4 to r.length-3) {

      val x0 = i
      val x1 = i+1

      val y0 = r.getDouble(i)
      val y1 = r.getDouble(i+1)

      val num = y1-y0
      val den = x1-x0


      var t = 0.0

      if (den ==0){
        t = 0.0
      }
      else{
        t = num/den
      }

      l+= atan(t)

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
