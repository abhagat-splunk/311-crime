//spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 --jars /home/cloudera/Shapefile-1.0-SNAPSHOT-jar-with-dependencies.jar
// Run the above command before executing the commands below
// compile jar using mvn clean compile assembly:single
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.udf

// Loading the ACS dataset with population statistics in a dataframe
val acs_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("bdad/ACS_16_5YR_DP05.csv")

// casting the population column from string to Int
val acs_df2 = acs_df.withColumn("Population", acs_df("HC01_VC03").cast(IntegerType))

// List of all valid zipcodes in NYC for five boroughs
val li = List(10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10043,10044,10045,10046,10047,10048,10055,10060,10069,10072,10079,10080,10081,10082,10087,10090,10094,10095,10096,10098,10099,10101,10102,10103,10104,10105,10106,10107,10108,10109,10110,10111,10112,10113,10114,10115,10116,10117,10118,10119,10120,10121,10122,10123,10124,10125,10126,10128,10129,10130,10131,10132,10133,10138,10149,10150,10151,10152,10153,10154,10155,10156,10157,10158,10159,10160,10161,10162,10163,10164,10165,10166,10167,10168,10169,10170,10171,10172,10173,10174,10175,10176,10177,10178,10179,10184,10185,10196,10197,10199,10203,10211,10212,10213,10242,10249,10256,10257,10258,10259,10260,10261,10265,10268,10269,10270,10271,10272,10273,10274,10275,10276,10277,10278,10279,10280,10281,10282,10285,10286,10292,10301,10302,10303,10304,10305,10306,10307,10308,10309,10310,10311,10312,10313,10314,10400,10451,10452,10453,10454,10455,10456,10457,10458,10459,10460,10461,10462,10463,10464,10465,10466,10467,10468,10469,10470,10471,10472,10473,10474,10475,10499,11001,11002,11003,11004,11005,11010,11020,11021,11022,11023,11024,11025,11026,11027,11030,11040,11041,11042,11043,11044,11050,11051,11052,11053,11054,11055,11096,11099,11201,11202,11203,11204,11205,11206,11207,11208,11209,11210,11211,11212,11213,11214,11215,11216,11217,11218,11219,11220,11221,11222,11223,11224,11225,11226,11228,11229,11230,11231,11232,11233,11234,11235,11236,11237,11238,11239,11240,11241,11242,11243,11244,11245,11247,11248,11249,11251,11252,11254,11255,11256)

// casting the Zipcode column from string to Int
val acs_df3 = acs_df2.withColumn("Zipcode", acs_df2("`GEO.id2`").cast(IntegerType))

// Filtering out only the zipcodes valid in NYC for the ACS dataset
val acs_df4 = acs_df3.filter($"Zipcode".isin(li:_* ))

// showing zipcode statistics
acs_df4.describe().show

// Loading the 311 dataset with complaint statistics in a dataframe
val df311 = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("bdad/311_Service_Requests_from_2011.csv")

// casting the Zipcode column from string to Int
val df311_2 = df311.withColumn("Zipcode", df311("Incident Zip").cast(IntegerType))

// Filtering out only the zipcodes valid in NYC for the 311 dataset
val df311_3 = df311_2.filter($"Zipcode".isin(li:_*))

// showing zipcode statistics
df311_3.describe().show()

// Joining the ACS and the 311 dataset based on the Zipcode column
val combined_1 = df311_3.join(acs_df4, Seq("Zipcode"), "inner")

combined_1.registerTempTable("combinedData")


// MVC DF1
val mvc_df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").load("bdad/MotorColl.csv")
var mvc_df2 = mvc_df1.na.drop(List("LATITUDE","LONGITUDE","LOCATION"))
mvc_df2 = mvc_df1.withColumn("Zipcode", mvc_df1("ZIP CODE").cast(IntegerType))
mvc_df2.printSchema()
val mvc_df3 = mvc_df2.filter($"Zipcode".isin(li:_* ))
//mvc_df3.describe().show()


def toZipcode(lat: String, long: String): Option[String] = {
  import java.io.IOException;
  import java.net.URISyntaxException;
  import org.opengis.referencing.FactoryException;
  import org.opengis.referencing.operation.TransformException;
  import praneeth.bdad.PointInPolygon;
  try { val zip = PointInPolygon.giveZip(lat, long);Some(zip) }
  catch { case e: Exception => println(e); None }
}

val coordinateToZip = udf(toZipcode _)

val mvc_df4 = mvc_df3.withColumn("zipcode_new", coordinateToZip.apply(col("LATITUDE"), col("LONGITUDE")))
