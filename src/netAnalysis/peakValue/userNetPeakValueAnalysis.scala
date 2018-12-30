package netAnalysis.peakValue

import common.{Constant, util}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable

//网络使用高低峰分析
object userNetPeakValueAnalysis{

  //声明文件的字段标题
  val hourOnlineCount = {
    for(i <- 0 to 23) yield{
      i.toString+"hourOnlineCount"
    }
  }
  val userNetHourAnalysisFileTitle = "userId,onlineYearMonth,semester,"+hourOnlineCount.mkString(",")

  //声明字段标题类型
  val titleType = List("String","String","String").toBuffer
  for(i <- 0 to 23){
    titleType.append("Int")
  }

  def transformDataList(line:Row) = {
      //从数据中读取每行的各个列的值
      val userId = line(0).toString
      val sex = line(1).toString
      val startTime = line(2).toString
      val endTime = line(3).toString

      //每小时上网时长默认值
      val hourOnlineCounts = {
        val buf = mutable.Buffer[Int]()
        for (i <- 1 to 24) {
          buf.append(0)
        }
        buf
      }

      //开始上网时间转成日期格式
      val startTimeDate = util.transformStrToCalender(startTime,"yyyy/MM/dd HH:mm")
      //结束上网时间转成日期格式
      val endTimeDate = util.transformStrToCalender(endTime,"yyyy/MM/dd HH:mm")

      //统计每条上网记录的上网小时数
      if (startTimeDate._3 == endTimeDate._3) {
        //上网时间为同一天，没有跨天
        for (hour <- startTimeDate._4 to endTimeDate._4) {
          hourOnlineCounts(hour) += 1
        }
      } else {
        val days = (endTimeDate._3 - startTimeDate._3)

        for (hour <- 0 to 23) {
          hourOnlineCounts(hour) += days - 1
        }

        //上网时间不在同一天
        for (hour <- startTimeDate._4 to 23) {
          hourOnlineCounts(hour) += 1
        }
        for (hour <- 0 to endTimeDate._4) {
          hourOnlineCounts(hour) += 1
        }
      }

      //把开始时间转成字符串格式，赋予变量 在线的年月
      val onlineYearMonth = startTimeDate._1.toString + startTimeDate._2.toString

      //获得学期
      val semester = util.getSemester(userId,startTimeDate)
      //返回键值对
      (hourOnlineCounts.toList,userId+"#"+onlineYearMonth+"#"+semester)
  }

  //把处理后的数据分解为要写入文件的形式
  def transformUserNetHourList(s:(String,List[Int])) = {
    var row:Row = null
    val userInfo = s._1.split("#")
    if(userInfo.length>0){
      val userId = userInfo(0)
      val onlineYearMonth = userInfo(1)
      val semester = userInfo(2)

      //先把三个固定字段放在列表中
      val list = List(userId,onlineYearMonth,semester)
      //与24小时的字段整合并转成Seq类型
      val seq = List.concat(list,s._2).toSeq
      //把seq转成Row类型
      row = Row.fromSeq(seq)
    }
    row
  }


  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = util.createSpark(Constant.MASTER,"userNetPeakValueAnalysis")

    //读取清洗后的数据并转成rdd
    val dataRdd = util.readCSVFile(spark,Constant.NETPATH + "washedNetData").rdd


    /**
      * 统计每个学生每个月份各个小时的上网时长
      */
    val hourCountsRdd = dataRdd
      //把每一行数据生成24个小时列
      .map(transformDataList)
      //根据需求选择字段
      .map(line=>(line._2,line._1))
      //合并相同key数据
      .reduceByKey{(x,y)=>
        val buffer = mutable.Buffer[Int]()
        for(i <- 0 to 23){
         buffer.append(x(i) + y(i))
        }
        buffer.toList
    }
    //转成能写入文件的形式
    .map(transformUserNetHourList)

    //写入文件
    util.writeFileByRDD(spark,hourCountsRdd,(userNetHourAnalysisFileTitle,titleType.toList),
      Constant.NETPATH + "userNetPeakValueAnalysis")
  }

}
