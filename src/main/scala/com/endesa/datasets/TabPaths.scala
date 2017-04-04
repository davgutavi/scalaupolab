package com.endesa.datasets

/**
  * Created by davgutavi on 15/03/17.
  */
object TabPaths {

  final val root = "/mnt/datos/ENDESA/"

//  final val prefix = "/media/davgutavi/Maxtor/ENDESA/endesa_descomprimido/"
  final val prefix = "/mnt/datos/ENDESA/endesa_descomprimido/"

  //Maestro Contratos
  final val TAB_00C = prefix+"TAB_00C/Endesa_TAB_00C_20170127_CZZ_20100101_20161231.csv"
  final val TAB_00C_headers = prefix+"TAB_00C/TAB_00C_headers.csv"

  //Maestro Aparatos
  final val TAB_00E = prefix+"TAB_00E/Endesa_TAB_00E_20170127_CZZ_20100101_20161231.csv"
  final val TAB_00E_headers = prefix+"TAB_00E/TAB_00E_headers.csv"

  //Curvas de Carga
  final val TAB_01_10 = prefix+"TAB_01/Endesa_TAB_01_20170127_CZZ_20100101_20101231.csv"
  final val TAB_01_11 = prefix+"TAB_01/Endesa_TAB_01_20170127_CZZ_20110101_20111231.csv"
  final val TAB_01_12 = prefix+"TAB_01/Endesa_TAB_01_20170127_CZZ_20120101_20121231.csv"
  final val TAB_01_13 = prefix+"TAB_01/Endesa_TAB_01_20170127_CZZ_20130101_20131231.csv"
  final val TAB_01_14 = prefix+"TAB_01/Endesa_TAB_01_20170127_CZZ_20140101_20141231.csv"
  final val TAB_01_15 = prefix+"TAB_01/Endesa_TAB_01_20170127_CZZ_20150101_20151231.csv"
  final val TAB_01_16 = prefix+"TAB_01/Endesa_TAB_01_20170127_CZZ_20160101_20161231.csv"
  final val TAB_01_headers = prefix+"TAB_01/TAB_01_headers.csv"

  //Consumos de Tipo I - IV
  final val TAB_03 = prefix+"TAB_03/Endesa_TAB_03_20170127_CZZ_20100101_20161231.csv"
  final val TAB_03_headers = prefix+"TAB_03/TAB_03_headers.csv"

  //Consumos de Tipo V
  final val TAB_04_10 = prefix+"TAB_04/Endesa_TAB_04_20170127_CZZ_20100101_20101231.csv"
  final val TAB_04_11 = prefix+"TAB_04/Endesa_TAB_04_20170127_CZZ_20110101_20111231.csv"
  final val TAB_04_12 = prefix+"TAB_04/Endesa_TAB_04_20170127_CZZ_20120101_20121231.csv"
  final val TAB_04_13 = prefix+"TAB_04/Endesa_TAB_04_20170127_CZZ_20130101_20131231.csv"
  final val TAB_04_14 = prefix+"TAB_04/Endesa_TAB_04_20170127_CZZ_20140101_20141231.csv"
  final val TAB_04_15 = prefix+"TAB_04/Endesa_TAB_04_20170127_CZZ_20150101_20151231.csv"
  final val TAB_04_16 = prefix+"TAB_04/Endesa_TAB_04_20170127_CZZ_20160101_20161231.csv"
  final val TAB_04_headers = prefix+"TAB_04/TAB_04_headers.csv"

  //Contratación
  final val TAB_05A = prefix+"TAB_05A/Endesa_TAB_05A_20170127_CZZ_20100101_20161231.csv"
  final val TAB_05A_headers = prefix+"TAB_05A/TAB_05A_headers.csv"

  //Geolocalización
  final val TAB_05B = prefix+"TAB_05B/Endesa_TAB_05B_20170127_CZZ_20100101_20161231.csv"
  final val TAB_05B_headers = prefix+"TAB_05B/TAB_05B_headers.csv"

  //Clientes
  final val TAB_05C = prefix+"TAB_05C/Endesa_TAB_05C_20170126_CZZ_20100101_20161231.csv"
  final val TAB_05C_headers = prefix+"TAB_05C/TAB_05C_headers.csv"

  //TDC
  final val TAB_15A = prefix+"TAB_15A/Endesa_TAB_15A_20170127_CZZ_20100101_20161231.csv"
  final val TAB_15A_headers = prefix+"TAB_15A/TAB_15A_headers.csv"

  //Operaciones TDC
  final val TAB_15B = prefix+"TAB_15B/Endesa_TAB_15B_20170127_CZZ_20100101_20161231.csv"
  final val TAB_15B_headers = prefix+"TAB_15B/TAB_15B_headers.csv"

  //Movimientos TDC
  final val TAB_15C = prefix+"TAB_15C/Endesa_TAB_15C_20170127_CZZ_20100101_20161231.csv"
  final val TAB_15C_headers = prefix+"TAB_15C/TAB_15C_headers.csv"

  //Expedientes
  final val TAB_16 = prefix+"TAB_16/Endesa_TAB_16_20170127_CZZ_20100101_20161231.csv"
  final val TAB_16_headers = prefix+"TAB_16/TAB_16_headers.csv"

  ///////////************Otros

  //Campañas


  //Clientes PTOSE
  final val TAB_05D = prefix+"TAB_05D/Endesa_TAB_05D_20170126_CZZ_20100101_20161231.csv"
  final val TAB_05D_headers = prefix+"TAB_05D/TAB_05D_headers.csv"

  //Magnitudes TPL
  final val TAB_10 = prefix+"TAB_10/Endesa_TAB_10_20170127_CZZ_20100101_20161231.csv"
  final val TAB_10_headers = prefix+"TAB_10/TAB_10_headers.csv"

  //Facturación
  final val TAB_12_10_12 = prefix+"TAB_12/Endesa_TAB_12_20170127_CZZ_DE_2010_A_2012.csv"
  final val TAB_12_12_13 = prefix+"TAB_12/Endesa_TAB_12_20170127_CZZ_DE_2012_A_2013.csv"
  final val TAB_12_14_15 = prefix+"TAB_12/Endesa_TAB_12_20170127_CZZ_DE_2014_A_2015.csv"
  final val TAB_12_16_17 = prefix+"TAB_12/Endesa_TAB_12_20170127_CZZ_DE_2016_A_2017.csv"
  final val TAB_12_headers = prefix+"TAB_12/TAB_12_headers.csv"



  //Interrupciones
  final val TAB_18_ene_mar_16 = prefix+"TAB_18/C3_Endesa_TAB_18_20160411_XZZ_20160123_20160331_1.csv"
  final val TAB_18_jul_16 = prefix+"TAB_18/C3_Endesa_TAB_18_20160712_XZZ_1.csv"
  final val TAB_18_ago_16 = prefix+"TAB_18/C3_Endesa_TAB_18_20160816_XZZ_1.csv"
  final val TAB_18_sep_16 = prefix+"TAB_18/C3_Endesa_TAB_18_20160921_XZZ_1.csv"
  final val TAB_18_oct_16 = prefix+"TAB_18/C3_Endesa_TAB_18_20161010_XZZ_1.csv"
  final val TAB_18_nov_16 = prefix+"TAB_18/C3_Endesa_TAB_18_20161108_XZZ_1.csv"
  final val TAB_18_dic_16 = prefix+"TAB_18/C3_Endesa_TAB_18_20161222_XZZ_1.csv"
  final val TAB_18_ene_17 = prefix+"TAB_18/C3_Endesa_TAB_18_20170111_XZZ_1.csv"
  final val TAB_18_headers = prefix+"TAB_18/TAB_18_headers.csv"




}