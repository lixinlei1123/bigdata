package netAnalysis.proportion

import common.{Constant, util}
import org.apache.spark.sql.{Row, SaveMode}

//分析上网程度
object userNetDegreeAnalysis {

  //最大熬夜时间
  var maxStayUpLateCount:Int = 0
  //最小熬夜时间
  var minStayUpLateCount:Int = 0
  //最大在线时间
  var maxOnlineDuration:Int = 0
  //最小在线时间
  var minOnlineDuration:Int = 0

  //声明文件的字段标题
  val userNetDegreeTitle = "userId,sex,onlineYearMonth,semester,netDegree"
  val titleType = List("String","String","String","String","Double")

  //生成要返回的数据
  def transformDataList(line:Row) = {
    //定义熬夜次数变量
    var stayUpLateCount = 0
    //学生id
    val userId = line(0).toString
    //学生性别
    val sex = line(1).toString
    //开始上网时间
    val startTime = line(2).toString
    //结束上网时间
    val endTime = line(3).toString

    //开始上网时间转成日期格式
    val startTimeDate = util.transformStrToCalender(startTime,"yyyy/MM/dd HH:mm")
    //结束上网时间转成日期格式
    val endTimeDate = util.transformStrToCalender(endTime,"yyyy/MM/dd HH:mm")

    //如果跨夜或者没跨夜但是23:30后下网的 熬夜次数加1
    val days = endTimeDate._3-startTimeDate._3
    stayUpLateCount += days
    if((endTimeDate._4>=23 && endTimeDate._5>=30)){
      stayUpLateCount += 1
    }

    //上网时长
    val onlineDuration = ((endTimeDate._6 - startTimeDate._6)/3600000.0)
    //把开始时间转成字符串格式，赋予变量 在线的年月
    val onlineYearMonth = startTimeDate._1.toString + startTimeDate._2.toString
    //获得学期
    val semester = util.getSemester(userId,startTimeDate)

    (userId+"#"+sex+"#"+onlineYearMonth+"#"+semester,(stayUpLateCount,onlineDuration.toInt))
  }

  //返回所有字段（把熬夜次数和上网时长展开）
  def transformNetList(s:(String,(Int,Int))) = {
    val userData = s._1.split("#")
    val userId = userData(0)
    val sex = userData(1)
    val onlineYearMonth = userData(2)
    val semester = userData(3)
    val stayUpLateCount = s._2._1
    val onlineDuration = s._2._2

    (userId,sex,stayUpLateCount,onlineDuration,onlineYearMonth,semester)
  }

  //算出本月学生上网能力值
  def transformNetDegree(x:(String,String,Int,Int,String,String)) = {
    val userId = x._1
    val sex = x._2
    val onlineYearMonth = x._5
    val semester = x._6

    //熬夜次数能力值
    val stayUpLateCount = util.normalize(minStayUpLateCount,maxStayUpLateCount,x._3)
    //在线时长能力值
    val onlineDuration = util.normalize(minOnlineDuration,maxOnlineDuration,x._4)
    //学生上网程度
    val userNetDegree = 100 * (onlineDuration * 0.7 + stayUpLateCount * 0.3)

    Row(userId,sex,onlineYearMonth,semester,userNetDegree)
  }


  def main(args: Array[String]): Unit = {
      val spark = util.createSpark(Constant.MASTER,"userNetDegreeAnalysis")
      val dataRdd = spark.read
          .option("header","true")
          .csv(Constant.NETPATH + "washedNetData")
          .rdd

      //生成学号，性别，熬夜次数，上网时长，上网年月，学期字段
      val userNetList = dataRdd
          .map(transformDataList)
          .reduceByKey{(x,y)=>
            (x._1 + y._1,x._2 + y._2)
          }.map(transformNetList)

      //计算出算上网程度所需要的参数
      //最大熬夜次数
      maxStayUpLateCount = userNetList.map(_._3).max()
      //最小熬夜次数
      minStayUpLateCount = userNetList.map(_._3).min()
      //最大在线时间
      maxOnlineDuration = userNetList.map(_._4).max().toInt
      //最小在线时间
      minOnlineDuration = userNetList.map(_._4).min().toInt
      //计算上网程度
      val linesRDD = userNetList.map(transformNetDegree)

      //写入文件
      util.writeFileByDF(spark,linesRDD,(userNetDegreeTitle,titleType),
        Constant.NETPATH + "userNetDegreeAnalysis")

      spark.stop()
  }

}
