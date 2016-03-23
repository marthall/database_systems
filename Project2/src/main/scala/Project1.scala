package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Project1 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MartinHallenProject1")

    val spark = new SparkContext(conf)

	if (args.length != 3) {
		println("USAGE: Project1 inputfolder outputfolder tasknr")
		exit(1)	
	}

	val N = 100 // Number of partitions

	val tasknr = args(2)

	val customers = spark.textFile(args(0) + "/customer.tbl").flatMap(x => {
		val fields = x.split('|')
		val customerKey = fields(0).toInt
		List((customerKey, customerKey))
	})
	val orders = spark.textFile(args(0) + "/orders.tbl").flatMap(x => {
		val fields = x.split('|')
		val orderKey = fields(0).toInt
		val customerKey = fields(1).toInt
		val orderDetails = fields(8)
		List((orderKey, customerKey, orderDetails))
	})

	// val customers = customers_raw.partitionBy(new org.apache.spark.HashPartitioner(200))
	// val orders = orders_raw.partitionBy(new org.apache.spark.HashPartitioner(200))

	println(customers.partitions.length)

	// val customers = spark.parallelize(customers_raw, 10)
	// val orders = spark.parallelize(orders_raw, 10)

	// val customes_transformed = customers.cartesian(spark.parallelize(i <- 0 to N))

	val regex = ".*special.*requests.*".r

	val t0 = System.nanoTime()

	val orderCount = orders.filter(x => !regex.pattern.matcher(x._3).matches)
							.map(order => (order._2, 1)).reduceByKey(_ + _)

	// val simpleCustomers = customers.map(customer => (customer(0),customer(0)))

	val customerOrders = customers.leftOuterJoin(orderCount)

	val orderCustomers = customerOrders.map(x => (x._2._2, x._2._1))

	val orderCountCustomers = orderCustomers.aggregateByKey(0)((a :Int, _) => a + 1,_+_)

	val result = orderCountCustomers.sortByKey()

	val cleanResult = result.map({
		case (Some(a:Int), b) => List(a, b).mkString("|")
		case (_, b) => List(0, b).mkString("|")
	})

	val t1 = System.nanoTime()
	val seconds = (t1 - t0)/1000000000.0

	println("Elapsed time: " + seconds + "seconds")

	cleanResult.collect.foreach(println)
	
	var outpath = ""

	if (tasknr=="1") {
		outpath = "/out_1"
	} else {
		outpath = "/out_2"
	}

	cleanResult.saveAsTextFile(args(1) + outpath)		

    spark.stop()
  }
}

