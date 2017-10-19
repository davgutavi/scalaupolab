package es.upo.datalab.datasets

import es.upo.datalab.entrypoints.s3_load.T16_dateParser.original_fields_01
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable

object MappingEstimadas extends MapFunction[Row, Row] {


    def call (r:Row):Row={


        val l = mutable.MutableList[Any]()

        original_fields_01( l, r )


        var cont = 0


        if (!r.getString(9).equalsIgnoreCase("R")) {cont+=1}


        if (!r.getString(17).equalsIgnoreCase("R")) {cont+=1}


        if (!r.getString(25).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(33).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(41).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(49).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(57).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(65).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(73).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(82).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(89).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(97).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(105).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(113).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(121).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(129).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(137).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(145).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(153).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(161).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(169).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(177).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(185).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(193).equalsIgnoreCase("R")) {cont+=1}
        if (!r.getString(201).equalsIgnoreCase("R")) {cont+=1}

        //numnoreales
        l += cont

        //consumdia
        l+= r.get(204)

        Row.fromSeq(l)
    }

    def original_fields_01(l: mutable.MutableList[Any], r: Row): Unit = {


        //cpuntmed
        l += r.get(0)

        //codpost
        l += r.get(1)

        //cenae
        l += r.get(2)

        //flectreg
        //l += r.get(3)

    }




}
