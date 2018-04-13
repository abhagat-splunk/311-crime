// spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
// Above command used to open spark shell in dumbo with the csv package loaded

import org.apache.spark.sql.types.IntegerType

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("bdad/ACS_16_5YR_DP05.csv")
val df2 = df.withColumn("HC01_VC03_tmp", df("HC01_VC03").cast(IntegerType))
df2.groupBy().min("HC01_VC03_tmp").show() //  result is 0 - minimum zipcode population
df2.groupBy().max("HC01_VC03_tmp").show() // result is 112982 - max xipcode population
df2.groupBy().avg("HC01_VC03_tmp").show() // result is 10977.509 - average zipcode population

// only profiled the total population statistics column and not other columns(which are stringLike)
// as that is the only column we need in our dataset. No need to clean as the column is pre-cleaned.