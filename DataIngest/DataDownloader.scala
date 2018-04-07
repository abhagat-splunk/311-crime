import sys.process._
import java.net.URL
import java.io.File

def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
}

val date = java.time.LocalDate.now

val 311zipFileName = "311data"+date+".zip"
val parkingViolationsFileName = "ParkingViolations"+date+".csv"

fileDownloader("https://data.cityofnewyork.us/download/fpz8-jqf4/application%2Fzip","311data"+date+".zip")
fileDownloader("https://data.cityofnewyork.us/api/views/2bnn-yakx/rows.csv?accessType=DOWNLOAD", parkingViolationsFileName)

("unzip -o "+311zipFileName).!

("hdfs dfs -put 311_Service_Requests_from_2011.csv 311Project/Data/").!
("hdfs dfs -put "+parkingViolationsFileName+" 311Project/Data").!
