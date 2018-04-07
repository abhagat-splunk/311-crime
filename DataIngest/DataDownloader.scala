import sys.process._
import java.net.URL
import java.io.File

def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
}

val date = java.time.LocalDate.now

val ThreeOneOneZipFileName = "311data"+date+".zip"
val parkingViolationsFileName = "ParkingViolations"+date+".csv"

fileDownloader("https://data.cityofnewyork.us/download/fpz8-jqf4/application%2Fzip",ThreeOneOneZipFileName)
fileDownloader("https://data.cityofnewyork.us/api/views/2bnn-yakx/rows.csv?accessType=DOWNLOAD", parkingViolationsFileName)

("unzip -o "+ThreeOneOneZipFileName).!

("hdfs dfs -put 311_Service_Requests_from_2011.csv 311Project/Data/").!
("hdfs dfs -put "+parkingViolationsFileName+" 311Project/Data").!
