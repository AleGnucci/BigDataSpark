package it.unibo.agnucci.spark.etl

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}

object EtlJob {

  //this is already available in spark shell
  private val spark = SparkSession
    .builder()
    .appName("Etl Phase")
    .getOrCreate()
  private val sc = spark.sparkContext

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val basePath = "hdfs:/user/agnucci/datasets/youtube-new/"
    val CAdf = readCsv(basePath + "CAvideos.csv")
    val DEdf = readCsv(basePath + "DEvideos.csv")
    val FRdf = readCsv(basePath + "FRvideos.csv")
    val GBdf = readCsv(basePath + "GBvideos.csv")
    val INdf = readCsv(basePath + "INvideos.csv")
    val JPdf = readCsv(basePath + "JPvideos.csv")
    val KRdf = readCsv(basePath + "KRvideos.csv")
    val MXdf = readCsv(basePath + "MXvideos.csv")
    val RUdf = readCsv(basePath + "RUvideos.csv")
    val USdf = readCsv(basePath + "USvideos.csv")

    /*bigDf1: files with only string fields, but MXdf and RUdf have uppercase representations of boolean fields,
    * while the others have "True" and "False" representations*/
    val bigDf1 = CAdf.union(DEdf.union(FRdf.union(GBdf.union(USdf.union(MXdf.union(RUdf))))))
    val bigDf2 = INdf.union(JPdf.union(KRdf)) //files with some string fields and some boolean fields
    val bigDf = correctDf1(bigDf1).union(bigDf2)

    val outputDf = bigDf.coalesce(1)

    val outputFolder = "hdfs:/user/agnucci/datasets/youtubeDataset"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputFolder), true)
    outputDf.write.parquet(outputFolder)
  }

  //Creates a DataFrame from a path.
  def readCsv(path: String): DataFrame =
    spark.read
      .option("wholeFile", true).option("multiline",true) //supports multiline fields (fields with \n)
      .option("mode", "DROPMALFORMED") //ignores badly formed records
      .option("header", "true").option("delimiter", ",") //specifies csv header presence and delimiter
      .option("inferSchema", "true")
      .csv(path)

  //Converts the fields that should be boolean to boolean.
  def correctDf1(df: DataFrame): DataFrame = {
    val dfStep1 = df.withColumn("comments_disabled", $"comments_disabled".cast(BooleanType))
    val dfStep2 = dfStep1.withColumn("ratings_disabled", $"ratings_disabled".cast(BooleanType))
    dfStep2.withColumn("video_error_or_removed", $"video_error_or_removed".cast(BooleanType))
  }
}
