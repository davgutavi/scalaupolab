package es.upo.datalab.entrypoints.s3_load

import java.sql.Date

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

import scala.collection.mutable

object T16_dateParser extends MapFunction[Row, Row] {

  /**
    * Fechas de la t16:
    * fapexpd [6]
    * finifran [7]
    * ffinfran [8]
    * fnormali [18]
    * fciexped [22]
    * @param row
    * @return
    */

  def call(row: Row): Row = {

    /**
      *Construyo un mutable list vacío que tendrá los nuevos valores de toda la fila
      */
    val l = mutable.MutableList[Any]()

    original_fields_01( l, row )

    //fapexpd
    l += check_date(row.getDate(6))
    //finifran
    l += check_date(row.getDate(7))
    //ffinfran
    l += check_date(row.getDate(8))

    original_fields_02( l, row )

    //fnormali
    l += check_date(row.getDate(18))

    original_fields_03( l, row )

    //fciexped
    l += check_date(row.getDate(22))

    Row.fromSeq( l )


  }


  /**
    * Chequeo de la fecha. Si es 0000-00-00 se cambia a 0001-01-01
    * @param source
    * @return
    */
  def check_date (source:Date):String = {

    var nf = source.toString

    if (source == null) {

      nf = "0001-01-01"

    }
    else {

      if (source.toString.equalsIgnoreCase("0000-00-00")) {

        nf = "0001-01-01"
      }

    }

    nf

  }



  /**
    * Almacenamos los elementos de la fila del 0 al 5
    *
    * @param l
    * @param row
    */

  def original_fields_01(l: mutable.MutableList[Any], row: Row): Unit = {


    //origen
    l += row.get( 0 )

    //cemptitu
    l += row.get( 1 )

    //cfinca
    l += row.get( 2 )

    //cptoserv
    l += row.get( 3 )

    //cderind
    l += row.get( 4 )

    //csecexpe
    l += row.get( 5 )


  }

  /**
    * Almacenamos los elementos de la fila del 9 al 17
    *
    * @param l
    * @param row
    */

  def original_fields_02(l: mutable.MutableList[Any], row: Row): Unit = {


    //anomalia
    l += row.get( 9 )

    //irregularidad
    l += row.get( 10 )

    //venacord
    l += row.get( 11 )

    //vennofai
    l += row.get( 12 )

    //torigexp
    l += row.get( 13)

    //texpedie
    l += row.get( 14)

    //expclass
    l += row.get( 14)
    //testexpe
    l += row.get( 16)
    //tpuntmed
    l += row.get( 17)
  }








  /**
    * Almacenamos los elementos de la fila del 19 al 21
    *
    * @param l
    * @param row
    */

  def original_fields_03(l: mutable.MutableList[Any], row: Row): Unit = {


    //cplan
    l += row.get( 19 )

    //ccampa
    l += row.get( 20 )

    //cempresa
    l += row.get( 21 )




  }

}