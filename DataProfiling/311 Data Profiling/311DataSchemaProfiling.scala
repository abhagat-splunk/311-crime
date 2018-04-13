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

val li = List(10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10043,10044,10045,10046,10047,10048,10055,10060,10069,10072,10079,10080,10081,10082,10087,10090,10094,10095,10096,10098,10099,10101,10102,10103,10104,10105,10106,10107,10108,10109,10110,10111,10112,10113,10114,10115,10116,10117,10118,10119,10120,10121,10122,10123,10124,10125,10126,10128,10129,10130,10131,10132,10133,10138,10149,10150,10151,10152,10153,10154,10155,10156,10157,10158,10159,10160,10161,10162,10163,10164,10165,10166,10167,10168,10169,10170,10171,10172,10173,10174,10175,10176,10177,10178,10179,10184,10185,10196,10197,10199,10203,10211,10212,10213,10242,10249,10256,10257,10258,10259,10260,10261,10265,10268,10269,10270,10271,10272,10273,10274,10275,10276,10277,10278,10279,10280,10281,10282,10285,10286,10292,10301,10302,10303,10304,10305,10306,10307,10308,10309,10310,10311,10312,10313,10314,10400,10451,10452,10453,10454,10455,10456,10457,10458,10459,10460,10461,10462,10463,10464,10465,10466,10467,10468,10469,10470,10471,10472,10473,10474,10475,10499,11001,11002,11003,11004,11005,11010,11020,11021,11022,11023,11024,11025,11026,11027,11030,11040,11041,11042,11043,11044,11050,11051,11052,11053,11054,11055,11096,11099,11201,11202,11203,11204,11205,11206,11207,11208,11209,11210,11211,11212,11213,11214,11215,11216,11217,11218,11219,11220,11221,11222,11223,11224,11225,11226,11228,11229,11230,11231,11232,11233,11234,11235,11236,11237,11238,11239,11240,11241,11242,11243,11244,11245,11247,11248,11249,11251,11252,11254,11255,11256)

//Filtering cases with unusual zipcodes
val dfFour = dfThree.filter($"IncidentZip".isin(li:_*))

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