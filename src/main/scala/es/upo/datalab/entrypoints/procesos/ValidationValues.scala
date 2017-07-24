package es.upo.datalab.entrypoints.procesos

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

object ValidationValues extends MapFunction[Row, Row] {

  def call(value: Row): Row = {


    val vconsume = value.getString( 10 )

    var sv = ""

    if (vconsume!=null){

      val vs = vconsume.split( "\\$" )
      sv = vs.map( l => {

        val w1 = l.split( ("\\|") )


        if (w1.length != 2) {
          "g"
        } else {
         w1( 1 ).toString
        }
      } ).mkString( "-" )

    }
    else{
      println(vconsume)
    }




    val rconsume = value.getString( 9 )

    var rv = ""

    if (rconsume!=null) {

//      println("rconsume = "+rconsume)

      val vr = rconsume.split( "\\$" )

//      println("vr = "+vr.mkString(" "))

      rv = vr.map( l => {

//        println("l = "+l)

        val w1 = l.split( ("\\|") )

//        println("w1 = "+w1.mkString("-")+" ["+w1.length+"]")

        if (w1.length != 2) {
         "g"
        } else {
          w1( 1 ).toString
        }
      } ).mkString( "-" )
    }
    else{
      println("return = "+rconsume)
    }







    val tconsume = value.getString( 16 )

    var tv = ""

    if (tconsume!=null) {

      val vt = tconsume.split( "\\$" )
      tv = vt.map( l => {

        val w1 = l.split( ("\\|") )


        if (w1.length != 2) {
          "g"
        } else {
          w1( 1 ).toString
        }
      } ).mkString( "-" )

    }
    else{
      println(tconsume)
    }


    Row( sv,rv,tv )


  }

}
