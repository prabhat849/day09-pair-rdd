import org.apache.spark.sql.SparkSession

object Day09 {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // 1. Create Spark Session
    // --------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day09-Pair-RDD")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext


    // --------------------------------------------------
    // 2. Create Pair RDD
    // --------------------------------------------------

    val sales = sc.parallelize(
      Seq(
        ("Laptop", 50000),
        ("Mobile", 20000),
        ("Laptop", 30000),
        ("Tablet", 15000),
        ("Mobile", 10000),
        ("Laptop", 20000)
      ),
      2
    )


    // --------------------------------------------------
    // 3. Display Original Pair RDD
    // --------------------------------------------------

    println("\n======================================")
    println("       DAY 09 - PAIR RDD")
    println("======================================")

    println("\nOriginal Sales Data:")

    sales.collect().foreach {
      case (product, revenue) =>
        println(product + " -> " + revenue)
    }


    // --------------------------------------------------
    // 4. reduceByKey()
    // --------------------------------------------------
    // Combines values having the same key
    //
    // Laptop = 50000 + 30000 + 20000 = 100000
    // Mobile = 20000 + 10000 = 30000
    // Tablet = 15000
    // --------------------------------------------------

    val revenueByProduct =
      sales.reduceByKey((a, b) => a + b)

    println("\n--------------------------------------")
    println("Revenue By Product - reduceByKey")
    println("--------------------------------------")

    revenueByProduct.collect().foreach {
      case (product, revenue) =>
        println(product + " -> " + revenue)
    }


    // --------------------------------------------------
    // 5. mapValues()
    // --------------------------------------------------
    // Adds 10% tax to the revenue
    // Key remains unchanged
    // --------------------------------------------------

    val revenueWithTax =
      revenueByProduct.mapValues(revenue => revenue * 1.10)

    println("\n--------------------------------------")
    println("Revenue Including 10% Tax")
    println("--------------------------------------")

    revenueWithTax.collect().foreach {
      case (product, revenue) =>
        println(product + " -> " + revenue)
    }


    // --------------------------------------------------
    // 6. groupByKey()
    // --------------------------------------------------
    // Groups all values belonging to the same key
    // --------------------------------------------------

    val groupedSales =
      sales.groupByKey()

    println("\n--------------------------------------")
    println("Sales Using groupByKey")
    println("--------------------------------------")

    groupedSales.collect().foreach {
      case (product, values) =>
        println(product + " -> " + values.mkString(", "))
    }


    // --------------------------------------------------
    // 7. Calculate Total Revenue
    // --------------------------------------------------

    val totalRevenue =
      revenueByProduct.map {
        case (_, revenue) => revenue
      }.sum()

    println("\n--------------------------------------")
    println("Total Revenue")
    println("--------------------------------------")

    println(totalRevenue)


    // --------------------------------------------------
    // 8. Find Maximum Revenue Product
    // --------------------------------------------------

    val maxRevenueProduct =
      revenueByProduct
        .map {
          case (product, revenue) =>
            (revenue, product)
        }
        .max()

    println("\n--------------------------------------")
    println("Highest Revenue Product")
    println("--------------------------------------")

    println(
      maxRevenueProduct._2 +
      " -> " +
      maxRevenueProduct._1
    )


    // --------------------------------------------------
    // 9. Count Number of Products
    // --------------------------------------------------

    val productCount =
      revenueByProduct.count()

    println("\n--------------------------------------")
    println("Number of Products")
    println("--------------------------------------")

    println(productCount)


    // --------------------------------------------------
    // 10. Sort Products By Revenue
    // --------------------------------------------------

    val sortedRevenue =
      revenueByProduct
        .map {
          case (product, revenue) =>
            (revenue, product)
        }
        .sortByKey(ascending = false)

    println("\n--------------------------------------")
    println("Products Sorted By Revenue")
    println("--------------------------------------")

    sortedRevenue.collect().foreach {
      case (revenue, product) =>
        println(product + " -> " + revenue)
    }


    // --------------------------------------------------
    // 11. RDD Partitions
    // --------------------------------------------------

    println("\n--------------------------------------")
    println("Number of Partitions")
    println("--------------------------------------")

    println(revenueByProduct.getNumPartitions)


    // --------------------------------------------------
    // 12. RDD Lineage
    // --------------------------------------------------

    println("\n--------------------------------------")
    println("RDD Lineage")
    println("--------------------------------------")

    println(revenueByProduct.toDebugString)


    // --------------------------------------------------
    // 13. reduceByKey vs groupByKey
    // --------------------------------------------------

    println("\n======================================")
    println("reduceByKey vs groupByKey")
    println("======================================")

    println("\nreduceByKey:")
    println("Combines values with the same key.")

    println("\ngroupByKey:")
    println("Groups all values with the same key.")


    // --------------------------------------------------
    // 14. Stop Spark
    // --------------------------------------------------

    spark.stop()
  }
}
