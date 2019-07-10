package it.unibo.spark.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object EtlJob {

  //this is already available in spark shell
  private val spark = SparkSession
    .builder()
    .appName("Etl Phase")
    .getOrCreate()

  import org.apache.spark.sql.types.StringType
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

    val bigDf1 = CAdf.union(DEdf.union(FRdf.union(GBdf.union(USdf.union(MXdf.union(RUdf)))))) //files with only string fields
    val bigDf2 = INdf.union(JPdf.union(KRdf)) //files with some string fields and some boolean fields
    val bigDf = bigDf1.union(correctDf(bigDf2))

    val outputDf = bigDf.coalesce(1)
    outputDf.write.parquet("hdfs:/user/agnucci/datasets/youtubeDataset")

    /*
    //code to create a small version of the dataset
    val CAdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/CAvideos.csv")
    correctDf(CAdf.limit(1000)).write.parquet("hdfs:/user/agnucci/datasets/youtubeDatasetSmall")
    */
  }

  /**
    * Converts boolean fields to String.
    * */
  def correctDf(df: DataFrame): DataFrame = {
    df.withColumn("comments_disabled", $"comments_disabled".cast(StringType))
      .withColumn("ratings_disabled", $"ratings_disabled".cast(StringType))
      .withColumn("video_error_or_removed", $"video_error_or_removed".cast(StringType))
  }

  /**
    * Creates a DataFrame from a path.
    * */
  def readCsv(path: String): DataFrame = {
    spark.read
      .option("wholeFile", true).option("multiline",true) //supports multiline fields (fields with \n)
      .option("mode", "DROPMALFORMED") //ignores badly formed records
      .option("header", "true").option("delimiter", ",") //specifies csv header presence and delimiter
      .option("inferSchema", "true")
      .csv(path)
  }

}
