import sys.process._
import java.net.URL
import java.io.File

def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
}

val date = java.time.LocalDate.now

val ThreeOneOneZipFileName = "311data"+date+".zip"
val parkingViolationsFileName = "ParkingViolations"+date+".csv"


//Download the 311 data
fileDownloader("https://data.cityofnewyork.us/download/fpz8-jqf4/application%2Fzip",ThreeOneOneZipFileName)

//Download the Parking Violations data
fileDownloader("https://data.cityofnewyork.us/api/views/2bnn-yakx/rows.csv?accessType=DOWNLOAD", parkingViolationsFileName)

//Unzip the 311 data 
("unzip -o "+ThreeOneOneZipFileName).!


//Put the data to HDFS
("hdfs dfs -put 311_Service_Requests_from_2011.csv 311Project/Data/").!
("hdfs dfs -put "+parkingViolationsFileName+" 311Project/Data").!
