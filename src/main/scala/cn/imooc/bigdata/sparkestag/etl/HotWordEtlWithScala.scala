package cn.imooc.bigdata.sparkestag.etl

import org.apache.spark.{SparkConf, SparkContext}

object HotWordEtlWithScala {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[1]")
      .setAppName("hot word scala")
    val sc = new SparkContext(config)
    val lines = sc.textFile("hdfs://10.1.1.121:8020/SogouQ.sample.txt")
    val total = lines.count()
    val hitCount = lines.map(x => x.split("\t")(3)) // 取数组中的第三个位置的元素（两个数字）
      .map(word => word.split(" ")(0).equals(word.split(" ")(1))) // 两个数字拆分并检查是否相等
      .filter(x => x == true).count()
    // 计算用户点击排名和网站排名相同网址所占的比例
    println("hit rate:" + (hitCount.toFloat / total.toFloat))
  }

}
