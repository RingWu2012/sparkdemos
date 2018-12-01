package wordcount

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * spark 核心是 RDD, RDD并不存储数据,而是存储计算信息
  * RDD的算子分为transformation、action
  * transformation不会触发任务执行，譬如 flatmap、map、reduceByKey、mappartition
  * action会触发任务执行，譬如 collect、countByKey
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    /**
      * 新建sparkconf，用于配置appname，master节点,
      * 在本地运行master 设置为 local[*]
      */
    val sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    //新建sparkContext
    val sparkContext = new SparkContext(sparkConf)

    /**
      * 使用textFile方法从hdfs中读取数据,还可以从其他数据源读取数据，具体请看官方文件
      * 文件路径可以写本地文件路径，前提是本地需要安装了hadoop，bin目录下有添加winutils相关dll，且配置了hadoop_home环境变量
      * 也可以写hdfs文件路径，譬如：hdfs：//hdp-01：8088/input/words.txt
      */
    val lines: RDD[String] = sparkContext.textFile("D:\\words.txt", 1)
    /**
      * flatmap可以将数据切分压平
      */
    val data = lines.flatMap(_.split(" "))
    /**
      * map对数据进行映射，譬如将（hello，world）映射为 （（hello，1），（world，1））
      */
    val mappedData = data.map((_, 1))
    /**
      * reduceByKey算子对相同key的数据进行聚合
      */
    val reducedData = mappedData.reduceByKey(_ + _)
    /**
      * 聚合完成后，根据数量进行排序，数量是第二个属性，所以是_._2;ascending=false代表降序
      */
    val sortedData = reducedData.sortBy(_._2, false)
    /**
      * collect是action，触发任务执行获取数据
      */
    val result = sortedData.collect()
    println(result.toBuffer)
    sparkContext.stop()
  }

}
