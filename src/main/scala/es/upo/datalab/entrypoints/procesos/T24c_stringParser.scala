package es.upo.datalab.entrypoints.procesos
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

import scala.collection.mutable

object T24c_stringParser extends MapFunction[Row, Row] {

  def call(row: Row): Row = {

    var wrong_xml_syntax = false

    if (row.getString( 7 ).toInt == 8) wrong_xml_syntax = true


    val l = mutable.MutableList[Any]()

    original_fields( l, row )

    println( "[ " + row.getString( 9 ) + " , " + row.getString( 10 ) + " , " + row.getString( 16 ) + " ]" )

    //raw
    new_fields( l, row.getString( 9 ), 42, wrong_xml_syntax )

    //validated
    new_fields( l, row.getString( 10 ), 68, wrong_xml_syntax )

    //time
    new_fields( l, row.getString( 16 ), 94, wrong_xml_syntax )

    //    if (wrong_xml_syntax)  println("["+l.length+"] "+l.mkString("#"))

    //    println("["+l.length+"] "+l.mkString("#"))


    Row.fromSeq( l )


  }


  def original_fields(l: mutable.MutableList[Any], row: Row): Unit = {


    //cups22
    l += row.get( 0 )

    //fechalectura
    l += row.get( 1 )

    //fecharecepcion
    l += row.get( 2 )

    //banderaverinv
    l += row.get( 3 )

    //consumototal
    l += row.get( 4 )

    //datavalidation
    l += row.get( 5 )

    //estadodesechada
    l += row.get( 6 )

    //estadovalidacion
    l += row.get( 7 )

    //fechasistema
    l += row.get( 8 )

    //informacionhorariabruta
    l += row.get( 9 )

    //informacionhorariaval
    l += row.get( 10 )

    //nhuecos
    l += row.get( 11 )

    //nreghorariosnovalidos
    l += row.get( 12 )

    //numeroserieequipo
    l += row.get( 13 )

    //periodicidad
    l += row.get( 14 )

    //tipomedida
    l += row.get( 15 )

    //validacionhoraria
    l += row.get( 16 )


  }


  def new_fields(l: mutable.MutableList[Any], s: String, position: Int, wrong_xml_syntax: Boolean): Unit = {

    if (!wrong_xml_syntax) {

      if (s != null) {

        val aux1 = s.split( "\\$" )

        val aux2 = aux1.map( i => {
          val w1 = i.split( ("\\|") )
          if (w1.length != 2 || w1( 0 ) == "") {
            l += "-1".toInt
          } else {

            try {
              l += w1( 0 ).toInt
            }
            catch {
              case num: NumberFormatException => {
                l += null
                println( "Error provocado por: " + w1( 0 ) )
              }
            }
          }
        } )

        val dif1 = position - l.length

        if (dif1 > 0) {

          for (a <- 0 until dif1) {
            l += "-1".toInt
          }
        }

        var aux3 = ""

        val aux4 = s.split( "\\$" )

        aux3 = aux4.map( i => {

          val w1 = i.split( ("\\|") )

          if (w1.length != 2 || w1( 1 ) == "") {
            "g"
          } else {
            w1( 1 ).toString
          }
        } ).mkString( "-" )

        val dif2 = 47 - aux3.length()

        if (dif2 > 0) {

          for (a <- 0 until dif2) {
            aux3 += "-g"
          }

        }

        l += aux3

      }


      else {


        for (i <- 0 until 26) {

          l += null

        }

      }

    }
    else {

      for (i <- 0 until 78) {

        l += null

      }


    }

  }

}
