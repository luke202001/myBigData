package com.myBigData.Spark_ClickStream

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.UUID;
import java.util.Date;

class WebLogSession {

}

object WebLogSession {

  val sdf_standard = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
  val sdf_hdfsfolder = new SimpleDateFormat("yyyy-MM-dd");

  //自定义的将日志信息按日志创建的时间升序排序
  def dateComparator(elementA:String ,elementB:String):Boolean = {
    WebLogSession.sdf_standard.parse(elementA.split(" ")(1)).getTime < WebLogSession.sdf_standard.parse(elementB.split(" ")(1)).getTime
  }

  import scala.collection.mutable.ListBuffer
  def distinctLogInfoBySession(logInfoGroup:List[String]):List[String] = {

    val logInfoBySession:ListBuffer[String] = new ListBuffer[String]
    var lastRequestTime:Long = 0;
    var lastSessionID:String = "";

    for(logInfo <- logInfoGroup) {

      //某IP的用户第一次访问网站的记录做为该用户的第一个session日志
      if(lastRequestTime == 0) {

        lastSessionID = UUID.randomUUID().toString();
        //将该次访问日志记录拼上sessionID并放进按session分类的日志信息数组中
        logInfoBySession += lastSessionID + " " +logInfo
        //记录该次访问日志的时间,并用户和下一条访问记录比较,看时间间隔是否超过30分钟,是的话就代表新Session开始
        lastRequestTime = sdf_standard.parse(logInfo.split(" ")(1)).getTime

      } else {

        //当前日志记录和上一次的访问时间相比超过30分钟,所以认为是一个新的Session,重新生成sessionID
        if(sdf_standard.parse(logInfo.split(" ")(1)).getTime - lastRequestTime >= 30 * 60 * 1000) {
          //和上一条访问记录相比,时间间隔超过了30分钟,所以当做一次新的session,并重新生成sessionID
          lastSessionID = UUID.randomUUID().toString();
          logInfoBySession += lastSessionID + " " +logInfo
          //记录该次访问日志的时间,做为一个新session开始的时间,并继续和下一条访问记录比较,看时间间隔是否又超过30分钟
          lastRequestTime = sdf_standard.parse(logInfo.split(" ")(1)).getTime

        } else { //当前日志记录和上一次的访问时间相比没有超过30分钟,所以认为是同一个Session,继续沿用之前的sessionID

          logInfoBySession += lastSessionID + " " +logInfo
        }
      }
    }
    return logInfoBySession.toList
  }

  def main(args: Array[String]) {



    val curDate = new Date();
    val logFile = "hdfs://192.168.1.161:9000/tmp/spark_clickstream/cleaned_log/"+WebLogSession.sdf_hdfsfolder.format(curDate) // Should be some file on your system
    val conf = new SparkConf().setAppName("WebLogSession").setMaster("local")
    val sc = new SparkContext(conf)
    val logFileSource = sc.textFile(logFile, 1).cache()

    //将log信息变为(IP,log信息)的tuple格式,也就是按IP地址将log分组
    val logLinesKVMapRDD = logFileSource.map(line => (line.split(" ")(0),line)).groupByKey();
    //对每个(IP[String],log信息[Iterator<String>])中的日志按时间的升序排序
    //(其实这一步没有必要,本来Nginx的日志信息就是按访问先后顺序记录的,这一步只是为了演示如何在Scala语境下进行自定义排序)
    //排完序后(IP[String],log信息[Iterator<String>])的格式变为log信息[Iterator<String>]
    val sortedLogRDD = logLinesKVMapRDD.map(_._2.toList.sortWith((A,B) => WebLogSession.dateComparator(A,B)))

    //将每一个IP的日志信息按30分钟的session分类并拼上session信息
    val logInfoBySessionRDD = sortedLogRDD.map(WebLogSession.distinctLogInfoBySession(_))
    //将List中的日志信息拆分成单条日志信息输出
    val logInfoWithSessionRDD =  logInfoBySessionRDD.flatMap(line => line).saveAsTextFile("hdfs://192.168.1.161:9000/tmp/spark_clickstream/session_log/"+WebLogSession.sdf_hdfsfolder.format(curDate))

  }
}