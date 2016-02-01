package com.example.chapter4

import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd

object SundayCount {

  def main (args: Array[String]) {
    if (args.length < 1) {
      throw new IllegalArgumentException ("/tmp/date.txt")
    }

    val filePath = args(0)
    val conf = new SparkConf
    val sc = new SparkContext(conf)


    println("try")
    try {
      val textRDD = sc.textFile(filePath)
      val dateTimeRDD = textRDD.map { dateStr =>
        val pattern =
          DateTimeFormat.forPattern("yyyyMMdd")
        DateTime.parse(dateStr, pattern)
      }
      val sundayRDD = dateTimeRDD.filter { dateTime =>
        dateTime.getDayOfWeek == DateTimeConstants.SUNDAY
      }
      val numOfSunday = sundayRDD.count
      println(s"与えられたデータの中に日曜日は${numOfSunday}個含まれていました。")
    } catch {
      case e: IllegalArgumentException      => println("Exception arg " + e)
      case e: IllegalStateException         => println("Exception sta " + e)
      case e: UnsupportedOperationException => println("Exception ope " + e)
    } finally {
      println("finally")
      sc.stop()
    }
  }

}
