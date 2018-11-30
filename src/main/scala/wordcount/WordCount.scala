package wordcount

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * spark 核心是 RDD, RDD并不存储数据,而是存储计算信息
  * RDD的算子分为transformation、action
  * transformation不会触发任务执行，譬如 flatmap、map、reduceByKey
  * action会触发任务执行，譬如 collect、countByKey
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")

    /**
      * 新建sparkconf，用于配置appname，master节点,
      * 在本地运行master 设置为 local[*]，
      * 如果用线上集群运行输入master的地址即可，譬如：spark：//hdp-02：7077
      */
    val sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    //新建sparkContext "main" java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps
    val sparkContext = new SparkContext(sparkConf)

    /**
      * 使用textFile方法从文件中读取数据,还可以从其他数据源读取数据，具体请看官方文件
      * 文件路径可以写本地文件路径，
      * 也可以写hdfs文件路径，譬如：hdfs：//hdp-01：8088/input/words.txt
      */
    val lines : RDD[String]= sparkContext.textFile("D:\\words.txt",1)
    /**
      * flatmap算子可以将数据切分压平
      */
    val data = lines.flatMap(_.split(" "))
    /**
      * map算子对数据进行映射，譬如将（hello，world）映射为 （（hello，1），（world，1））
      */
    val mappedData = data.map((_,1))
    /**
      * reduceByKey算子对相同key的数据进行聚合
      */
     val reducedData = mappedData.reduceByKey(_+_)
    /**
      * collect是action，触发任务执行获取数据
      */
    val result = reducedData.collect()
//    println(result)
    sparkContext.stop()
  }

}
