package es.upo.datalab.entrypoints.s3_load

import java.sql.Date

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row

import scala.collection.mutable

object T05a_dateParser extends MapFunction[Row, Row] {

  /**
    * Fechas de la t05a:
    *fsolicon [8]
    *faltacon [9]
    *fbajacon [10]
    *fpsercon [11]
    *ffinvesu [12]
    *fvtocon  [13]
    * @param row
    * @return
    */

  def call(row: Row): Row = {

    /**
      *Construyo un mutable list vacío que tendrá los nuevos valores de toda la fila
      */
    val l = mutable.MutableList[Any]()

    original_fields_01(l,row)

    //fsolicon
    l += check_date(row.getDate(8))
    //faltacon
    l += check_date(row.getDate(9))
    //fbajacon
    l += check_date(row.getDate(10))
    //fpsercon
    l += check_date(row.getDate(11))
    //ffinvesu
    l += check_date(row.getDate(12))
    //fvtocon
    l += check_date(row.getDate(13))

    original_fields_02(l,row)

    //fadscri1
    l += check_date(row.getDate(19))
    //fvalads1
    l += check_date(row.getDate(20))

    //vpotads2
    l += row.get(21)

    //fadscri2
    l += check_date(row.getDate(22))
    //fvalads2
    l += check_date(row.getDate(23))

    //vpotads3
    l += row.get(24)

    //fadscri3
    l += check_date(row.getDate(25))
    //fvalads3
    l += check_date(row.getDate(26))

    original_fields_03(l,row)

    //fboletin
    l += row.get(39)

    original_fields_04(l,row)

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
    * Almacenamos los elementos de la fila del 0 al 7
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

    //tcontrat
    l += row.get(4)

    //testcont
    l += row.get(5)

    //ctarifa
    l += row.get(6)

    //cnae
    l += row.get(7)

  }

  /**
    * Almacenamos los elementos de la fila del 14 al 18
    *
    * @param l
    * @param row
    */

  def original_fields_02(l: mutable.MutableList[Any], row: Row): Unit = {

    //csubsect
    l += row.get(14)

    //vnumcerr
    l += row.get(15)

    //tubiapar
    l += row.get(16)

    //csectmie
    l += row.get(17)

    //vpotads1
    l += row.get(18)

  }


    /**
    * Almacenamos los elementos de la fila del 25 al 36
    *
    * @param l
    * @param row
    */

  def original_fields_03(l: mutable.MutableList[Any], row: Row): Unit = {

    //tconcort
    l += row.get(27)

    //testader
    l += row.get(28)

    //ccliente
    l += row.get(29)

    //vpotppal
    l += row.get(30)

    //potencia 1
    l += row.get(31)

    //potencia 2
    l += row.get(32)

    //potencia 3
    l += row.get(33)

    //potencia 4
    l += row.get(34)

    //potencia 5
    l += row.get(35)

    //potencia 6
    l += row.get(36)

    //empresa_instaladora
    l += row.get(37)

    //instalador
    l += row.get(38)


  }


  /**
    * Almacenamos los elementos de la fila del 40 al 44
    *
    * @param l
    * @param row
    */

  def original_fields_04(l: mutable.MutableList[Any], row: Row): Unit = {

    //tension
    l += row.get(40)

    //fases
    l += row.get(41)

    //vpmaxbie
    l += row.get(42)

    //emergencia
    l += row.get(43)


  }

}
