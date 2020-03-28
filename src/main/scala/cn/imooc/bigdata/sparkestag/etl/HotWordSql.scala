package cn.imooc.bigdata.sparkestag.etl

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object HotWordSql {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[1]")
      .setAppName("hot word scala")
    val sc = new SparkContext(config)
    val lines = sc.textFile("hdfs://10.1.1.121:8020/SogouQ.sample.txt")
    val row = lines.map(line => {
      var arr = line.split("\t")
      val str = arr(3)
      val rank_click = str.split(" ")
      Row(rank_click(0).toInt, rank_click(1).toInt)
    })

    val structType = StructType(
      StructField("rank", IntegerType, false) ::
        StructField("click", IntegerType, false) :: Nil
    )

    val ss = SparkSession.builder().getOrCreate()
    val df = ss.createDataFrame(row,structType)
    df.createOrReplaceTempView("tb")
    val re = df.sqlContext.sql("select count(if(t.rank=t.click,1,null)) as hit" +
      ", count(1) as total from tb as t ")
    re.show()
    val next = re.toLocalIterator().next()
    val hit = next.getAs[Long]("hit")  //注意转换完后为lang类型
    val total = next.getAs[Long]("total")
//    print("========="+hit+"====="+total)
    println(hit.toFloat/total.toFloat)

  }

}
