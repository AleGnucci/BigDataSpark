import org.apache.spark.sql.{DataFrame, SparkSession}

object EtlJob {

  //this is already available in spark shell
  private val spark = SparkSession
    .builder()
    .appName("Etl Phase")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.types.StringType

  def main(args: Array[String]): Unit = {

    val CAdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/CAvideos.csv")
    val DEdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/DEvideos.csv")
    val FRdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/FRvideos.csv")
    val GBdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/GBvideos.csv")
    val INdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/INvideos.csv")
    val JPdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/JPvideos.csv")
    val KRdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/KRvideos.csv")
    val MXdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/MXvideos.csv")
    val RUdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/RUvideos.csv")
    val USdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/USvideos.csv")

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
