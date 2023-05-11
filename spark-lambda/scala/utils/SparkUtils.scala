package utils

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

object SparkUtils {
  val isIDE = {
    "IDE CONFIGS"
  }
  def getSparkContext(appName: String) = {
    var checkpointDirectory = ""

    // get spark config 
    val conf = new SparkConf()
      .setAppName(appName)

    // check if is running from IDE 
    // research for VSCODE
    if(isIDE) {
      "IDE CONFIGS"
      checkpointDirectory = "file:///f:/temp"
    } else {
      checkpointDirectory = "hdfs://lambda:9000/spark/checkpoint"
    }

    // setup spark context
    val sc = new SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  } 

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration : Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp =>  ssc.checkpoint(cp))
  }
}