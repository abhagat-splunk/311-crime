import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.types.IntegerType

val sqlContext = new SQLContext(sc)

//Creating a dataframe
val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("311Project/Data/311_Service_Requests_from_2011.csv")

//Casting Unique Key and Incident Zip to IntegerType to perform Integertype profiling
val dfTwo = df.withColumn("UniqueKey",df("Unique Key").cast(IntegerType))
val dfThree = dfTwo.withColumn("IncidentZip",df("Incident Zip").cast(IntegerType))

//Filtering cases with unusual zipcodes
val dfFour = dfThree.filter($"IncidentZip" < 15000)

//Printing the schema of the database
dfFour.printSchema()

//Profiling for the type casted integertype columns
dfFour.describe().show()


//Profiling for the relevant columns

//Max length of the strings in each column
dfFour.agg(max(length($"Created Date"))).as[Int].first
dfFour.agg(max(length($"Closed Date"))).as[Int].first
dfFour.agg(max(length($"Agency"))).as[Int].first
dfFour.agg(max(length($"Agency Name"))).as[Int].first
dfFour.agg(max(length($"Complaint Type"))).as[Int].first
dfFour.agg(max(length($"Descriptor"))).as[Int].first
dfFour.agg(max(length($"Location Type"))).as[Int].first
dfFour.agg(max(length($"Incident Zip"))).as[Int].first
dfFour.agg(max(length($"Incident Address"))).as[Int].first
dfFour.agg(max(length($"Street Name"))).as[Int].first
dfFour.agg(max(length($"Address Type"))).as[Int].first
dfFour.agg(max(length($"City"))).as[Int].first
dfFour.agg(max(length($"Landmark"))).as[Int].first
dfFour.agg(max(length($"Status"))).as[Int].first
dfFour.agg(max(length($"Due Date"))).as[Int].first
dfFour.agg(max(length($"Borough"))).as[Int].first
dfFour.agg(max(length($"X Coordinate (State Plane)"))).as[Int].first
dfFour.agg(max(length($"Y Coordinate (State Plane)"))).as[Int].first
dfFour.agg(max(length($"Latitude"))).as[Int].first
dfFour.agg(max(length($"Longitude"))).as[Int].first
dfFour.agg(max(length($"Location"))).as[Int].first

//Min length of the strings in each column
dfFour.agg(min(length($"Created Date"))).as[Int].first
dfFour.agg(min(length($"Closed Date"))).as[Int].first
dfFour.agg(min(length($"Agency"))).as[Int].first
dfFour.agg(min(length($"Agency Name"))).as[Int].first
dfFour.agg(min(length($"Complaint Type"))).as[Int].first
dfFour.agg(min(length($"Descriptor"))).as[Int].first
dfFour.agg(min(length($"Location Type"))).as[Int].first
dfFour.agg(min(length($"Incident Zip"))).as[Int].first
dfFour.agg(min(length($"Incident Address"))).as[Int].first
dfFour.agg(min(length($"Street Name"))).as[Int].first
dfFour.agg(min(length($"Address Type"))).as[Int].first
dfFour.agg(min(length($"City"))).as[Int].first
dfFour.agg(min(length($"Landmark"))).as[Int].first
dfFour.agg(min(length($"Status"))).as[Int].first
dfFour.agg(min(length($"Due Date"))).as[Int].first
dfFour.agg(min(length($"Borough"))).as[Int].first
dfFour.agg(min(length($"X Coordinate (State Plane)"))).as[Int].first
dfFour.agg(min(length($"Y Coordinate (State Plane)"))).as[Int].first
dfFour.agg(min(length($"Latitude"))).as[Int].first
dfFour.agg(min(length($"Longitude"))).as[Int].first
dfFour.agg(min(length($"Location"))).as[Int].first

//Avg length of the strings in each column
dfFour.agg(avg(length($"Created Date"))).as[Double].first
dfFour.agg(avg(length($"Closed Date"))).as[Double].first
dfFour.agg(avg(length($"Agency"))).as[Double].first
dfFour.agg(avg(length($"Agency Name"))).as[Double].first
dfFour.agg(avg(length($"Complaint Type"))).as[Double].first
dfFour.agg(avg(length($"Descriptor"))).as[Double].first
dfFour.agg(avg(length($"Location Type"))).as[Double].first
dfFour.agg(avg(length($"Incident Zip"))).as[Double].first
dfFour.agg(avg(length($"Incident Address"))).as[Double].first
dfFour.agg(avg(length($"Street Name"))).as[Double].first
dfFour.agg(avg(length($"Address Type"))).as[Double].first
dfFour.agg(avg(length($"City"))).as[Double].first
dfFour.agg(avg(length($"Landmark"))).as[Double].first
dfFour.agg(avg(length($"Status"))).as[Double].first
dfFour.agg(avg(length($"Due Date"))).as[Double].first
dfFour.agg(avg(length($"Borough"))).as[Double].first
dfFour.agg(avg(length($"X Coordinate (State Plane)"))).as[Double].first
dfFour.agg(avg(length($"Y Coordinate (State Plane)"))).as[Double].first
dfFour.agg(avg(length($"Latitude"))).as[Double].first
dfFour.agg(avg(length($"Longitude"))).as[Double].first
dfFour.agg(avg(length($"Location"))).as[Double].first