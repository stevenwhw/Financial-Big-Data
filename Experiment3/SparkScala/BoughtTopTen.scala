import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object BoughtTopTen {
  def main(args : Array[String]) = {
    val conf = new SparkConf().setAppName("BoughtTopTen")
    val sc = new SparkContext(conf)
    val input = "hdfs://h01:9000/user/MuserInput/million_user_log.csv"
    val buy="2"
    val texts = sc.textFile(input).map(line => line.split("\n"))
      .flatMap(words =>
      {

        words.map(word =>
        {if(word.split(",")(7)==buy)
          (word.split(",")(1)+","+word.split(",")(10), 1)
        else
          (word.split(",")(1)+","+word.split(",")(10), 0)
        })
      }
      )
    val counts =  texts.reduceByKey(_ + _)
    counts.collect.foreach{
      case (word, num) =>
        { if(num>10)
          println(word + " " + num.toString)}
    }
    val res=counts.groupBy(_._1.toString.split(",")(1)).map(word=>{
      val temp=word._2.toArray.sortBy(_._2).reverse
      //      val z:Array[String] = new Array[String](10)
      var ten = ""
      for(i<-0 to 9){
        ten=ten+temp(i)._1.split(",")(0)+","+temp(i)._2.toString+" "
      }
      val pro=word._1
      (pro,ten)
    })
    res.collect.foreach{
      case (word, num) =>
      {
        println(word + " " + num.toString)}
    }
    res.saveAsTextFile("hdfs://h01:9000/user/BoTop/")
//    val eachpro=counts.map(word=>{
//      val province=word._1.split(",")(1)
//      val after=word._1.split(",")(0)+","+word._2.toString
//      (province,after)
//    })
//    val sorted=eachpro.groupByKey().sortBy(_._2.toString.split(",")(1).toInt,false)
//    sorted.collect.foreach{
//      case (word, num) =>{
//      { if(num.toString.split(",")(1).toInt>0)
//        println(word + " " + num.toString)}
//    }
  }
}
