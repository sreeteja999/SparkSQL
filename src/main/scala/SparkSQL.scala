package scala.main

import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex


object SparkSQL {
  def main(args: Array[String]){

      //create basic SparkSession
      val session = SparkSession.builder().master("local").appName("Join table").getOrCreate()

      import session.implicits._

      // Connect to local host
      session.conf.set("fs.defaultFS","hdfs://localhost:9000/user/sreeteja")

      // Read json file and create dataframe
      val df = session.read.json("hdfs://localhost:9000/user/sreeteja/samplejson.json")

      df.show()

      // Read logfile and create rdd using SparkContext
      val data = session.sparkContext.textFile("hdfs://localhost:9000/user/sreeteja/logfile.log")

      // Create regular expression and match data in logfile and filter out
      val regPattern : Regex = "([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{1,3}) ([A-Za-z]{4,5}) ([A-Za-z0-9.]{0,100}:) ([A-Za-z0-9 !@#$%^&*()_-]{0,200})".r

      val filteredlog = data
       // .filter(x=> x.matches("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{1,3} [A-Za-z]{4,5} [A-Za-z0-9.]{0,100}: [A-Za-z0-9\\ !@#$%^&*()_-]{0,200}"));
        .filter(x => x.matches(regPattern.pattern.pattern()))

      case class logfile(date:String,time:String,logtype:String,method:String,message:String)

      val name = filteredlog.map( file => {
        val regPattern(a, b, c, d, e) = file
        (a, b, c, d, e)
      }).toDF()

      name.show()

    }
}
