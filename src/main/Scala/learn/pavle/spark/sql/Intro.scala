package learn.pavle.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * 入门，编写测试方法
 */
class Intro {

  @Test
  def rddIntro(): Unit ={
    val conf = new SparkConf().setMaster("local[6]").setAppName("rddIntro")
    val sc = new SparkContext(conf)

    //词频统计
    sc.textFile("dataset/wordcount.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(print(_))

    /**
     * 运行结果：
     * (scala,1)(hive,1)(zookeeper,1)(kafka,1)(spark,2)(hadoop,3)(flume,1)
     */

  }

  @Test
  def dsIntro(): Unit ={
    val spark = new SparkSession.Builder()
      .appName("dsIntro")
      .master("local[6]")
      .getOrCreate()

    //toDS 隐式转换
    import spark.implicits._
    val sourceRDD = spark.sparkContext.parallelize(Seq(
      Person("zhangsan",10),
      Person("lisi",15),
      Person("wangwu",25)))
    val personDS = sourceRDD.toDS()

    //查询年龄大于10岁小于20岁的姓名
    val resultDS = personDS.where( 'age > 10 )
      .where( 'age < 20 )
      .select( 'name )
      .as[String]

    resultDS.show()

    /**
     * 运行结果：
     * +----+
     * |name|
     * +----+
     * |lisi|
     * +----+
     */

  }

}

case class Person(name:String, age:Int)
