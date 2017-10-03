package es.upo.datalab.entrypoints.s3_load

import java.sql.Date

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

import scala.collection.mutable

object T05c_dateParser extends MapFunction[Row, Row] {

  /**
    * Fechas de la t05c:
    * fechamov [5]
    * @param row
    * @return
    */

  def call(row: Row): Row = {

    /**
      *Construyo un mutable list vacío que tendrá los nuevos valores de toda la fila
      */
    val l = mutable.MutableList[Any]()

    original_fields_01(l,row)

    //fechamov
    l += check_date(row.getDate(5))

    original_fields_02(l,row)


    Row.fromSeq(l)

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
    * Almacenamos los elementos de la fila del 0 al 4
    *
    * @param l
    * @param row
    */

  def original_fields_01(l: mutable.MutableList[Any], row: Row): Unit = {


    //origen
    l += row.get(0)

    //cemptitu
    l += row.get(1)

    //ccontrat
    l += row.get(2)

    //cnumscct
    l += row.get(3)

    //ccliente
    l += row.get(4)

  }

  /**
    * Almacenamos los elementos de la fila del 6 al 9
    *
    * @param l
    * @param row
    */

  def original_fields_02(l: mutable.MutableList[Any], row: Row): Unit = {

    //tindfiju
    l += row.get(6)

    //cnifdnic
    l += row.get(7)

    //dapersoc
    l += row.get(8)

    //dnombcli
    l += row.get(9)


  }



}

