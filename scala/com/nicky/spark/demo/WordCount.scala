package com.nicky.spark.demo

import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.hive.HiveContext

object WordCount extends App { 

  case class person(person: String, Age: Int)

  val inputFile = "C:\\Users\\35108\\test\\Overview.md"
  val outputFile = "C:\\Users\\35108\\output\\"
  System.setProperty("hadoop.home.dir", "D:\\Softwares\\hadoop");
  val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
  // Create a Scala Spark Context.
  val sc = new SparkContext(conf)
  
  sc.textFile("")
  
  def x(y:String)(z:Int) ={    
    y
  }
  
  val a = x("s")(_)
  
  a(10)
  
  
  val sql = new SQLContext(sc)
  val hc = new HiveContext(sc)
  // Load our input data.
  val input = sc.textFile(inputFile)
  // Split up into words.
  val words = input.flatMap(line => line.split(" "))
  // Transform into word and count.
  val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    .map(word => (word._1.toString(), word._2.toString())).map(p => Row(p._1.toString(), p._2.toInt))

  //def row(line: List[String]): Row = Row(line(0), line(1).toInt)

  // val data = counts.map(_.split(",").to[List]).map(row)

  def dfSchema(columnNames: Seq[String]): StructType =
    StructType(
      Seq(
        StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "age", dataType = IntegerType, nullable = false)))

  val schema = dfSchema(Seq("name", "age"))
  //val schema = Seq("name1" ,"age1").mkString
  val df = hc.createDataFrame(counts, schema)
  //val r = sql.createDataFrame(counts, schema)

  val windowSpects = Window.partitionBy("name").orderBy("age")

  df.withColumn("G>2", count("name").over(windowSpects)).show
  //sql.createDataFrame(counts,schema).show()

  //counts.foreach(println)
  // Save the word count back out to a text file, causing evaluation.
  //counts.saveAsTextFile(outputFile)

}