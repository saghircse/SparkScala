// Databricks notebook source

// Dataset Using Range
val ds = spark.range(3)
ds.show()

// Dataset Using Sequence
val ds = Seq(11, 22, 33).toDS()
ds.show()

// Dataset Using List
val ds = List(10,20,30).toDS
ds.show()

// Dataset Using Sequence of Case Classes
case class Book(name: String, cost: Int)
val bookDS = Seq(Book("Scala", 400), Book("Spark", 500), Book("Kafka", 300)).toDS()
bookDS.show()

// Dataset Using RDD
val rdd = sc.parallelize(Seq(("Spark",500), ("Scala",400),("Kafka",300)))
val integerDS = rdd.toDS()
integerDS.show()

// Dataset from Dataframe using Case Class
// Seq[Book] -> RDD[Book] -> Dataframe -> Dataset[Book]
case class Book(name: String, cost: Int)
val bookSeq = Seq(Book("Scala", 400), Book("Spark", 500), Book("Kafka", 300))
val bookRDD = sc.parallelize(bookSeq)
val bookDF = bookRDD.toDF()
val bookDS = bookDF.as[Book]
bookDS.show()

// Dataset from Dataframe using Tuples
// Seq[(String, Int)] -> RDD[(String, Int)] -> Dataframe -> Dataset[(String, Int)]
val bookSeq = Seq(("Scala", 400), ("Spark", 500), ("Kafka", 300))
val bookRDD = sc.parallelize(bookSeq)
val bookDF = bookRDD.toDF("Id", "Name")
val bookDS = bookDF.as[(String, Int)]
bookDS.show()

// Word Count Example using Dataset
val linesDS = sc.parallelize(Seq("Spark is fast", "Spark has Dataset", "Spark Dataset is typesafe")).toDS()
val wordsDS = linesDS.flatMap(_.toLowerCase.split(" ")).filter(_ != "")
val groupedDS = wordsDS.groupBy("value")
val countsDS = groupedDS.count()
countsDS.show()

// Convert Dataset to Dataframe
val countsDF = countsDS.toDF.orderBy($"count" desc) 
countsDF.show()
