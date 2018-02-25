package com.myBigData.Spark_ClickStream

import scala.io.Source
import java.text.SimpleDateFormat;
import java.util.Locale;
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Date;

class WebLogClean extends Serializable {

  def weblogParser(logLine:String):String =  {

    //过滤掉信息不全或者格式不正确的日志信息
    val isStandardLogInfo = logLine.split(" ").length >= 12;

    if(isStandardLogInfo) {

      //过滤掉多余的符号
      val newLogLine:String = logLine.replace("- - ", "").replaceFirst("""\[""", "").replace(" +0000]", "");
      //将日志格式替换成正常的格式
      val logInfoGroup:Array[String] = newLogLine.split(" ");
      val oldDateFormat = logInfoGroup(1);
      //如果访问时间不存在，也是一个不正确的日志信息
      if(oldDateFormat == "-") return ""
      val newDateFormat = WebLogClean.sdf_standard.format(WebLogClean.sdf_origin.parse(oldDateFormat))
      return newLogLine.replace(oldDateFormat, newDateFormat)

    } else {

      return ""

    }
  }
}

object WebLogClean {

  val sdf_origin = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
  val sdf_standard = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
  val sdf_hdfsfolder = new SimpleDateFormat("yyyy-MM-dd");

  def main(args: Array[String]) {

    val curDate = new Date();
    val weblogclean = new WebLogClean
    val logFile = "hdfs://192.168.1.161:9000/tmp/flume/events/"+WebLogClean.sdf_hdfsfolder.format(curDate)+"/*" // Should be some file on your system
    val conf = new SparkConf().setAppName("WebLogCleaner").setMaster("local")
    val sc = new SparkContext(conf)
    val logFileSource = sc.textFile(logFile,1).cache()

    val logLinesMapRDD = logFileSource.map(x => weblogclean.weblogParser(x)).filter(line => line != "");

    logLinesMapRDD.saveAsTextFile("hdfs://192.168.1.161:9000/tmp/spark_clickstream/cleaned_log/"+WebLogClean.sdf_hdfsfolder.format(curDate))

  }

}
