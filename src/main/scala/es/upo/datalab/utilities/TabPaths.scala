package es.upo.datalab.utilities

/**
  * Created by davgutavi on 15/03/17.
  */
object TabPaths {

  //HDFS Laboratorio
  final val rootHDFS = "hdfs://192.168.47.247/user/gutierrez/endesa/"
  final val prefix_database = rootHDFS+"database_parquet/"
  final val prefix_datasets = rootHDFS+"datasets_parquet/"

  //HD PC LABORATORIO
  final val rootHDPCLAB = "/mnt/datos/recursos/ENDESA/"
  final val headersHDPCLAB = rootHDPCLAB+"headers/"
  final val rootHDPCLABAbsolutas = rootHDPCLAB+"absolutas/descomprimidas/"

  //HD PORTATIL LAB
  final val rootHDUPO = "/media/davgutavi/Maxtor/ENDESA/"
  final val headersHDUPO = rootHDUPO+"headers/"
  final val rootHDUPOAbsolutas = rootHDPCLAB+"absolutas/descomprimidas/"

  //HD PORTATIL US
  final val rootHDUS = "/media/davgutavi/ushdportatil/entregas/"
  final val headersHDUS = rootHDUS+"headers/"
  final val rootAbsolutas = rootHDUS+"absolutas/"
  final val rootIncrementales = rootHDUS+"incrementales/"
  final val rootAbsolutasComprimidas = rootHDUS+"absolutas/comprimidas/"

  //MAC David
  final val rootHDMio = "/Volumes/david/endesa/"
  final val prefixHDMio = "/Volumes/david/endesa/base_de_datos_descomprimida/"
  final val headersHDMio = prefixHDMio+"headers/"

  //#################################################################################
  //#################################################################################

  //******************************************TAB00C
  final val TAB00C = prefix_database+"TAB00C"
  final val TAB00C_headers =  headersHDUS+"TAB00C_headers.csv"
  final val TAB00C_csv = rootAbsolutasComprimidas+"TAB00C"
  final val TAB00C_csv_hdpclab = rootHDPCLABAbsolutas+"TAB00C"
  final val TAB00C_headers_hdpclab =  headersHDPCLAB+"TAB00C_headers.csv"

  final val TAB00C_csv_hdupo = rootHDUPOAbsolutas+"TAB00C"
  final val TAB00C_headers_hdupo =  headersHDUPO+"TAB00C_headers.csv"


  //******************************************TAB00E
  final val TAB00E = prefix_database+"TAB00E"
  final val TAB00E_headers =  headersHDUS+"TAB00E_headers.csv"
  final val TAB00E_csv = rootAbsolutasComprimidas+"TAB00E"

  //******************************************TAB01
  final val TAB01 = prefix_database+"TAB01"
  final val TAB01_headers =  headersHDUS+"TAB001_headers.csv"
  final val TAB01_csv = rootAbsolutasComprimidas+"TAB01"

  //******************************************TAB02
  final val TAB02 = prefix_database+"TAB02"
  final val TAB02_headers = headersHDUS+"TAB02_headers.csv"
  final val TAB02_csv = rootAbsolutasComprimidas+"TAB02"

  //******************************************TAB05A
  final val TAB05A = prefix_database+"TAB05A"
  final val TAB05A_headers =  headersHDUS+"TAB05A_headers.csv"
  final val TAB05A_csv = rootAbsolutasComprimidas+"TAB05A"

  //******************************************TAB05B
  final val TAB05B = prefix_database+"TAB05B"
  final val TAB05B_headers =  headersHDUS+"TAB05B_headers.csv"
  final val TAB05B_csv = rootAbsolutasComprimidas+"TAB05B"

  //******************************************TAB05C
  final val TAB05C = prefix_database+"TAB05C"
  final val TAB05C_headers =  headersHDUS+"TAB05C_headers.csv"
  final val TAB05C_csv = rootAbsolutasComprimidas+"TAB05C"

  //******************************************TAB05D
  final val TAB05D = prefix_database+"TAB05D"
  final val TAB05D_headers =  headersHDUS+"TAB05D_headers.csv"
  final val TAB05D_csv = rootAbsolutasComprimidas+"TAB05D"

  //******************************************TAB06
  final val TAB06 = prefix_database+"TAB06"
  final val TAB06_headers =  headersHDUS+"TAB06_headers.csv"
  final val TAB06_csv = rootAbsolutasComprimidas+"TAB06"

  //******************************************TAB08
  final val TAB08 = prefix_database+"TAB08"
  final val TAB08_headers =  headersHDUS+"TAB08_headers.csv"
  final val TAB08_csv = rootAbsolutasComprimidas+"TAB08"

  //******************************************TAB15A
  final val TAB15A = prefix_database+"TAB15A"
  final val TAB15A_headers =  headersHDPCLAB+"TAB15A_headers.csv"
  final val TAB15A_csv = rootAbsolutasComprimidas+"TAB15A"

  //******************************************TAB15B
  final val TAB15B = prefix_database+"TAB15B"
  final val TAB15B_headers =  headersHDUS+"TAB15B_headers.csv"
  final val TAB15B_csv = rootAbsolutasComprimidas+"TAB15B"

  //******************************************TAB15C
  final val TAB15C = prefix_database+"TAB15C"
  final val TAB15C_headers_hdpclab =  headersHDPCLAB+"TAB15C_headers.csv"
  final val TAB15C_csv_hdpclab = rootHDPCLABAbsolutas+"TAB15C"

  //******************************************TAB16
  final val TAB16 = prefix_database+"TAB16"
  final val TAB16_headers =  headersHDUS+"TAB16_headers.csv"
  final val TAB16_csv = rootAbsolutasComprimidas+"TAB16"

  //******************************************TAB24
  final val TAB24 = prefix_database+"TAB24"
  final val TAB24_headers =  headersHDUS+"TAB24_headers.csv"
  final val TAB24_csv = rootAbsolutasComprimidas+"TAB24"

}