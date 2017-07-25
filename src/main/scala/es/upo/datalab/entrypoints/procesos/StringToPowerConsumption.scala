package es.upo.datalab.entrypoints.procesos

import java.util.regex.Pattern

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

import scala.collection.mutable

object StringToPowerConsumption extends MapFunction[Row,Row] {

  def call(row: Row): Row = {


    val l = mutable.MutableList[Any]()


    fase1(l,row)



//    println("[ "+row.getString(9)+" , "+row.getString(10)+" , "+row.getString(16)+" ]")

    //raw
    mew_fields(l,row.getString(9),42)

    //validated
    mew_fields(l,row.getString(10),68)

    //time
    mew_fields(l,row.getString(16),94)

//    println("["+l.length+"] "+l.mkString("#"))


    Row.fromSeq(l)


  }




   def fase1 (l:mutable.MutableList[Any], row:Row):Unit = {

     //cups22
     l+=row.get(0)

     //fechalectura
     l+=row.get(1)

     //fecharecepcion
     l+=row.get(2)

     //banderaverinv
     l+=row.get(3)

     //consumototal
     l+=row.get(4)

     //datavalidation
     l+=row.get(5)

     //estadodesechada
     l+=row.get(6)

     //estadovalidacion
     l+=row.get(7)

     //fechasistema
     l+=row.get(8)

     //informacionhorariabruta
     l+=row.get(9)

     //informacionhorariaval
     l+=row.get(10)

     //nhuecos
     l+=row.get(11)

     //nreghorariosnovalidos
     l+=row.get(12)

     //numeroserieequipo
     l+=row.get(13)

     //periodicidad
     l+=row.get(14)

     //tipomedida
     l+=row.get(15)

     //validacionhoraria
     l+=row.get(16)

   }


  def mew_fields(l:mutable.MutableList[Any], s:String, position:Int):Unit = {


    if(s != null) {

      val aux1 = s.split( "\\$" )

      val aux2 = aux1.map( i => {
        val w1 = i.split( ("\\|") )
        if (w1.length != 2) {
          l += "-1".toInt
        } else {
          l += w1( 0 ).toInt
        }
      } )

      //    println("length = "+l.length)


      val dif1 = position - l.length

      //    println("position = "+position)
      //    println("length = "+l.length)
      //    println("dif1 = "+dif1)

      if (dif1 > 0) {

        for (a <- 0 until dif1) {
          l += "-1".toInt
        }
      }

      var aux3 = ""

      val aux4 = s.split( "\\$" )

      aux3 = aux4.map( i => {

        val w1 = i.split( ("\\|") )

        if (w1.length != 2) {
          "g"
        } else {
          w1( 1 ).toString
        }
      } ).mkString( "-" )


      //    println("length ="+aux3.length())


      val dif2 = 47 - aux3.length()

      //    println("position = 47")
      //    println("length = "+ aux3.length())
      //    println("dif2 = "+dif2+"\n\n")

      if (dif2 > 0) {

        for (a <- 0 until dif2) {
          aux3 += "-g"
        }

      }

      l += aux3


    }
    else{


      for(i <- 0 until 26 ){

        l += null

      }

    }



  }

  }
