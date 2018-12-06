package sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    /**
      * sparkContext不能创建特殊的RDD（DataFrame） sqlContext将SparkContext包装进而增强
      */
    val sqlContext = new SQLContext(sc)
    //可以从本地读取文件，也可以从hdfs读取文件,没有hdfs请写本地文件路径，譬如：D：/users.txt
    val lines = sc.textFile("hdfs://hdp-01:9000/user/root/users.txt")
    //将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(" ")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      Row(id, name, age)
    })
    //结果类型，其实就是表头，用于描述DataFrame
    val sch: StructType = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    //将RowRDD关联schema
    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, sch)
    //把DataFrame先注册临时表
    bdf.registerTempTable("t_user")
    //书写SQL（SQL方法应其实是Transformation）
    val result: DataFrame = sqlContext.sql("SELECT * FROM t_user ORDER BY age asc")
    //查看结果（触发Action）
    result.show()
    sc.stop()
  }
}
