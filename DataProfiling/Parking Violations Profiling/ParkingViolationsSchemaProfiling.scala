import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.types.IntegerType

val sqlContext = new SQLContext(sc)

//Creating a dataframe
val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("311Project/Data/ParkingViolations2018-04-06.csv")
val dfTwo = df.withColumn("SummonsNumber",df("Summons Number").cast(IntegerType))
val dfThree = dfTwo.withColumn("ViolationPrecinct",df("Violation Precinct").cast(IntegerType))
val dfFour = dfThree.filter($"ViolationPrecinct" < 125)

//Trying for more accurate filter with meter number, can be used later on (not to be used now)
val dfFive = dfFour.filter(length($"Meter Number") > 1).select("Meter Number")

//Printing the schema of the database
dfFour.printSchema()

//Profiling for the type casted integertype columns
dfFour.describe().show()


//Profiling for the relevant columns

//Max length of the strings in each column
dfFour.agg(max(length($"Summons Number"))).as[Int].first
dfFour.agg(max(length($"Plate ID"))).as[Int].first
dfFour.agg(max(length($"Registration State"))).as[Int].first
dfFour.agg(max(length($"Issuing Agency"))).as[Int].first
dfFour.agg(max(length($"Issue Date"))).as[Int].first
dfFour.agg(max(length($"Violation Location"))).as[Int].first
dfFour.agg(max(length($"Violation Precinct"))).as[Int].first
dfFour.agg(max(length($"Violation Time"))).as[Int].first
dfFour.agg(max(length($"Violation County"))).as[Int].first
dfFour.agg(max(length($"Issuer Code"))).as[Int].first
dfFour.agg(max(length($"House Number"))).as[Int].first
dfFour.agg(max(length($"Street Name"))).as[Int].first
dfFour.agg(max(length($"Intersecting Street"))).as[Int].first

//Min length of the strings in each column
dfFour.agg(min(length($"Summons Number"))).as[Int].first
dfFour.agg(min(length($"Plate ID"))).as[Int].first
dfFour.agg(min(length($"Registration State"))).as[Int].first
dfFour.agg(min(length($"Issuing Agency"))).as[Int].first
dfFour.agg(min(length($"Issue Date"))).as[Int].first
dfFour.agg(min(length($"Violation Location"))).as[Int].first
dfFour.agg(min(length($"Violation Precinct"))).as[Int].first
dfFour.agg(min(length($"Violation Time"))).as[Int].first
dfFour.agg(min(length($"Violation County"))).as[Int].first
dfFour.agg(min(length($"Issuer Code"))).as[Int].first
dfFour.agg(min(length($"House Number"))).as[Int].first
dfFour.agg(min(length($"Street Name"))).as[Int].first
dfFour.agg(min(length($"Intersecting Street"))).as[Int].first

//Avg length of the strings in each column
dfFour.agg(avg(length($"Summons Number"))).as[Double].first
dfFour.agg(avg(length($"Plate ID"))).as[Double].first
dfFour.agg(avg(length($"Registration State"))).as[Double].first
dfFour.agg(avg(length($"Issuing Agency"))).as[Double].first
dfFour.agg(avg(length($"Issue Date"))).as[Double].first
dfFour.agg(avg(length($"Violation Location"))).as[Double].first
dfFour.agg(avg(length($"Violation Precinct"))).as[Double].first
dfFour.agg(avg(length($"Violation Time"))).as[Double].first
dfFour.agg(avg(length($"Violation County"))).as[Double].first
dfFour.agg(avg(length($"Issuer Code"))).as[Double].first
dfFour.agg(avg(length($"House Number"))).as[Double].first
dfFour.agg(avg(length($"Street Name"))).as[Double].first
dfFour.agg(avg(length($"Intersecting Street"))).as[Double].first

