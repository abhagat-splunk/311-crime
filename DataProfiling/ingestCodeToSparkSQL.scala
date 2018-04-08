import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext 

val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("311Project/Data/311_Service_Requests_from_2011.csv")

val dfTwo = df.withColumn("UniqueKey",df("Unique Key").cast(IntegerType))
val dfThree = dfTwo.withColumn("IncidentZip",df("Incident Zip").cast(IntegerType))

val dfFour = dfThree.filter($"IncidentZip" < 15000)

