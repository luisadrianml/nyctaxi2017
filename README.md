# NYC Taxi 2017
## Analysis through Big Data techniques
1. Spark
2. Spark SQL
3. Google Cloud Platform: Dataproc

### Obtaining Data
Data can be downloaded from NYC TLC website. From NYC TLC data will be given by month (January, February, March...).

Website to download: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
Website to download the taxi zone: https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

### Data
1. 12 files (1 per month)
2. 9.3GB of data
3. 112,494,978 rows in dataset

### Tools used
1. Google Cloud Platform: Dataproc
2. Scala
3. Spark SQL

### Preprocessing
1. Filtered null values
2. Joined dataset with Taxi Zone from NYC TLC
3. Creation of new columns
4. Only pickup and dropoff from 2017
5. Calculating time for queries

### How to run
#### Variables
Before, it is important to specify the location where all the 12 CSV files are, and the TaxiZone file. 

Location for 2017 can be modified from variable "allmonths", and for file Taxizone is "taxizone" variable.

#### Running
In order to run the code, it is important to run it in the Spark interpreter with Scala. 

### Analysis 
#### Analysis on trips overall
1. Average distance travelled
2. Average time travelled
3. Efficiency based on distance over time
4. Average passenger per ride
5. Average tip per ride
6. Average total paid per trip
7. Total trips per hour
8. Total trips per month
9. Common pickup locations
#### Analysis based on time and location
1. Average trips per day of week
2. Average distance travelled by time of day
3. Average distance by time of day per borough
4. Average tips by time and day of the week
5. Pickup location by season
6. Percentage pickup and drop off per borough
7. General drop off locations vs. Nightlife drop off locations
#### Analysis based on fare/payment
1. Most common payment type per borough
2. Most common location per payment type per borough
3. Most common payment type according to hour
4. Average gross revenue per hour

### Performance Evaluation
An evaluation of the time it took to run every script was made. We used the function System.NanoTime before the script, and after. Difference was saved into a file and the average was calculated in Excel to add to paper.