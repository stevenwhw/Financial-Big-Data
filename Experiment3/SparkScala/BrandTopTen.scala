import java.io.FileWriter

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object BrandTopTen {
  def main(args : Array[String]) = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val input = "hdfs://h01:9000/user/MuserInput/million_user_log.csv"
    val action="0"
    val texts = sc.textFile(input).map(line => line.split("\n"))
      .flatMap(words =>
      {

        words.map(word =>
        {if(word.split(",")(7)==action)
          (word.split(",")(4), 1)
        else
          (word.split(",")(4), 0)
        })
      }
      )
    val counts =  texts.reduceByKey(_ + _).sortBy(_._2,false)
//    val out = new FileWriter("output/test.txt",true)
//    val arr=new Array[String](10)
//    for(i<-0 to 9){
//      println(counts.collect.toList(i))
//      out.write(counts.collect.toList(i)._1+"  "+counts.collect.toList(i)._2.toString+"\n")
//      arr(i)=counts.collect.toList(i)._1+"  "+counts.collect.toList(i)._2.toString+"\n"
//    }
//    out.close()
    val res=sc.parallelize(counts.top(10))
    res.saveAsTextFile("hdfs://h01:9000/user/BrandTop/")
  }
}

