package com.sh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

object SparkMain {
	def main(args: Array[String]): Unit = {
	  val spark = SparkSession
	    .builder()
			.appName("Spark Example")
			.master("local")
			//.config("spark.some.config.option", "some-value")
			.getOrCreate()
		
		val sc=spark.sparkContext
		sc.setLogLevel("ERROR")
		val sqlContext = spark.sqlContext
		
		import spark.implicits._

		// define UDAF
		val customMean = new CustomMean()

		// create test dataset
		val data = (1 to 10).map{x:Int => x match {
		  case t if t <= 5 => Row("A", t.toDouble)
			case t => Row("B", t.toDouble)
		}}

		// create schema of the test dataset
		val schema = StructType(Array(
		  StructField("key", StringType),
			StructField("value", DoubleType)
		))

		// construct data frame
		val rdd = sc.parallelize(data)
		val df = sqlContext.createDataFrame(rdd, schema)
					
		println("===============>INPUT DATAFRAME");
		df.show()

		//================================== UDAF Example - Mean ===============================
		// Calculate average value for each group
		// Way-1 : Using DQL
		val df1=df.groupBy("key").agg( customMean(df.col("value")).as("custom_mean"), avg("value").as("avg"))
		println("===============>OUTPUT DATAFRAME : Mean UDAF Way-1");
		df1.show()
		
		// Way-2 : Using Spark SQL
		df.createOrReplaceTempView("dft")
		sqlContext.udf.register("customMean",customMean)
		val df2 =  sqlContext.sql("select key,customMean(value) as custom_mean, avg(value) as avg from dft group by key")
		println("===============>OUTPUT DATAFRAME : Mean UDAF Way-2");
		df2.show()
		
		
		//================================== UDF Example - Multiply ===============================
		
		// Example of simple UDF -- not UDAF
		def multiplyX(n:Double,x:Int) : Double = {
		 val y= n*x
		 y
		}
		
		def multiply2(n:Double) : Double = {
		 val y= n*2
		 y
		}
		
		val multiX = udf{
		  (x:Double,n:Int) => multiplyX(x,n)
		}
		sqlContext.udf.register("multiX",multiX)
		sqlContext.udf.register("multiply2",multiply2(_))
		sqlContext.udf.register("multiplyX",multiplyX(_,_))
		
		val df3 =  sqlContext.sql("select key,value,multiplyX(value,3) as multiplyX from dft")
		println("===============>OUTPUT DATAFRAME : UDF Example");
		df3.show()
		

	}
}