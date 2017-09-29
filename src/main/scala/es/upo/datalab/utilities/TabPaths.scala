package es.upo.datalab.utilities

/**
  * Created by davgutavi on 15/03/17.
  */
object TabPaths {

  //Output paths
  final val output_root = "hdfs://192.168.47.247/user/sparkOutputs/"

  //HDFS Laboratorio
  final val hdfs_root = "hdfs://192.168.47.247/user/datos/endesa/"
  final val hdfs_database_parquet = hdfs_root+"database_parquet/"
  final val hdfs_datasets_parquet = hdfs_root+"datasets_parquet/"
  final val hdfs_headers = hdfs_root+"headers/"

  //PC LABORATORIO
  final val pclab_root = "/mnt/datos/recursos/ENDESA/"
  final val pclab_database_parquet = pclab_root+"database_parquet/"
  final val pclab_headers = hdfs_root+"headers/"

  //HD PORTATIL LAB
  final val hdlab_root = "/media/davgutavi/Maxtor/ENDESA/"
  final val hdlab_database_parquet = hdlab_root+"database_parquet/"

  //HD PORTATIL US
  final val hdus_root = "/media/cluster/ushdportatil/"
  final val hdus_database_parquet = hdus_root+"database_parquet/"

  //MAC David
  final val hdmio_root = "/Volumes/david/endesa/"
  final val hdmio_database_parquet = hdmio_root+"database_parquet/"


  //#################################################################################
  //#################################################################################

  //******************************************TAB00C
  final val TAB00C = hdfs_database_parquet+"TAB00C"
  final val TAB00C_hdfs_headers = hdfs_headers+"TAB00C_headers.csv"
  final val TAB00C_pclab_headers = pclab_headers+"TAB00C_headers.csv"

  //******************************************TAB00E
  final val TAB00E = hdfs_database_parquet+"TAB00E"

  //******************************************TAB01
  final val TAB01 = hdfs_database_parquet+"TAB01"

  //******************************************TAB02
  final val TAB02 = hdfs_database_parquet+"TAB02"

  //******************************************TAB05A
  final val TAB05A = hdfs_database_parquet+"TAB05A"

  //******************************************TAB05B
  final val TAB05B = hdfs_database_parquet+"TAB05B"

  //******************************************TAB05C
  final val TAB05C = hdfs_database_parquet+"TAB05C"

  //******************************************TAB06
  final val TAB06 = hdfs_database_parquet+"TAB06"

  //******************************************TAB08
  final val TAB08 = hdfs_database_parquet+"TAB08"

  //******************************************TAB15A
  final val TAB15A = hdfs_database_parquet+"TAB15A"

  //******************************************TAB15B
  final val TAB15B = hdfs_database_parquet+"TAB15B"

  //******************************************TAB15C
  final val TAB15C = hdfs_database_parquet+"TAB15C"

  //******************************************TAB16
  final val TAB16 = hdfs_database_parquet+"TAB16"

  //******************************************TAB24
  final val TAB24 = hdfs_database_parquet+"TAB24"

}