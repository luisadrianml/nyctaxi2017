package com.bdp.nyctaxi

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark._


object NYCTaxi2017 {
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("NYCTaxi2017")
        val sc = new SparkContext(conf)
        val spark = new org.apache.spark.sql.SQLContext(sc)

        
        /// Storage Unit
        val storageDirection = "gs://dataproc-1f071807-a7c0-44d8-9545-456bff2a82af-us/"
        
        /// Command to save .write.format("csv").option("header","true").save(storageDirection+"/results/")
        
        val schema = StructType(Array(
                      StructField("VendorID", IntegerType, true), 
                      StructField("tpep_pickup_datetime", TimestampType, true), 
                      StructField("tpep_dropoff_datetime", TimestampType, true), 
                      StructField("passenger_count", IntegerType, true), 
                      StructField("Trip_distance", DoubleType, true), 
                      StructField("RatecodeID", IntegerType, true), 
                      StructField("store_and_fwd_flag", StringType,true),
                      StructField("PULocationID", IntegerType,true),
                      StructField("DOLocationID", IntegerType,true),
                      StructField("payment_type", IntegerType,true),
                      StructField("fare_amount", DoubleType,true),
                      StructField("extra", DoubleType,true),
                      StructField("mta_tax", DoubleType,true),
                      StructField("tip_amount", DoubleType,true),
                      StructField("tolls_amount", DoubleType,true),
                      StructField("improvement_surcharge", DoubleType,true),
                      StructField("total_amount", DoubleType,true)
                      ))
        
        val schemaTaxiZone = StructType(Array(
                      StructField("locationID", IntegerType, true), 
                      StructField("borough", StringType, true), 
                      StructField("zone", StringType, true), 
                      StructField("service_zone", StringType, true)
                      ))
        
        // READING THE NYC Taxi Sample
        // val allmonths = spark.read.option("header","true").schema(schema).csv(storageDirection + "/project/nSample1.csv/sample1.csv")
        // allmonths.createOrReplaceTempView("allmonths")
        
        // READING THE NYC Taxi Sample Locally
        // val allmonths = spark.read.option("header","true").schema(schema).csv("/project/sample1.csv")
        // allmonths.createOrReplaceTempView("allmonths")
        
        // Reading the NYC Taxi DataSet
        val allmonths = spark.read.option("header","true").schema(schema).csv(storageDirection + "/*.csv")
        val allmonthsFiltered = allmonths.filter($"tpep_pickup_datetime".isNotNull).filter($"tpep_dropoff_datetime".isNotNull)
        allmonthsFiltered.createOrReplaceTempView("allmonths")
        
        // Reading the Taxi Zones
        val taxizone = spark.read.option("header","true").schema(schemaTaxiZone).csv(storageDirection + "taxizone/taxizone_lookup.csv")
        taxizone.createOrReplaceTempView("taxizone")
        
        // Reading the Taxi Zones LOCALLLY
        // val taxizone = spark.read.option("header","true").schema(schemaTaxiZone).csv("project/taxizone.csv")
        // taxizone.createOrReplaceTempView("taxizone")
        
        // Joined data
        var joined = spark.sql("SELECT * FROM allmonths LEFT OUTER JOIN taxizone ON allmonths.PULocationID = taxizone.LocationID")
        
        // Fixing Joining
        val fixJoined = joined.withColumnRenamed("borough","PU_Borough").withColumnRenamed("zone","PU_Zone").drop("locationID").drop("service_zone")
        fixJoined.createOrReplaceTempView("PU_Location_Joined")
        var joinedDROP = spark.sql("SELECT * FROM PU_Location_Joined LEFT OUTER JOIN taxizone ON PU_Location_Joined.DOLocationID = taxizone.LocationID")
        val fixJoinDROP = joinedDROP.withColumnRenamed("borough","DO_Borough").withColumnRenamed("zone","DO_Zone").drop("locationID").drop("service_zone")
        
        // creating column for difference of secs
        val diff_secs_col = col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")
        
        //adding all column for pickup
        val temp = fixJoinDROP.withColumn("PUMonth", month(fixJoinDROP("tpep_pickup_datetime"))).withColumn("PUDay",dayofmonth(fixJoinDROP("tpep_pickup_datetime"))).withColumn("PUWeekday",date_format(fixJoinDROP("tpep_pickup_datetime"),"EEEE")).withColumn("PUHour",hour(fixJoinDROP("tpep_pickup_datetime"))).withColumn("PUMinute",minute(fixJoinDROP("tpep_pickup_datetime")))
        
        val temp1 = temp.withColumn("DOMonth", month(temp("tpep_dropoff_datetime"))).withColumn("DODay",dayofmonth(temp("tpep_dropoff_datetime"))).withColumn("DOWeekday",date_format(temp("tpep_dropoff_datetime"),"EEEE")).withColumn("DOHour",hour(temp("tpep_dropoff_datetime"))).withColumn("DOMinute",minute(temp("tpep_dropoff_datetime"))).withColumn("trip_duration",diff_secs_col)
        
//////////////////////////// Deleting mistakes of years
        val temp2 = temp1.filter(year($"tpep_pickup_datetime").equalTo(2017)).filter(year($"tpep_dropoff_datetime").equalTo(2017))
        
        
////////////// EXAMPLE OF WHAT I ENCOUNTERED (If you do count of all and only count =2017, difference is 1896. It means, 1896 values do not have 2017 as year.)
//        spark.sql("select count(*) from data where year(tpep_dropoff_datetime)>2017").show
//+--------+                                                                      
//|count(1)|
//+--------+
//|    1778|
//+--------+
//        +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+----------+--------------------+----------+--------------------+-------+-----+---------+------+--------+-------+-----+---------+------+--------+-------------+
//|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|Trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|PU_Borough|             PU_Zone|DO_Borough|             DO_Zone|PUMonth|PUDay|PUWeekday|PUHour|PUMinute|DOMonth|DODay|DOWeekday|DOHour|DOMinute|trip_duration|
//+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+----------+--------------------+----------+--------------------+-------+-----+---------+------+--------+-------+-----+---------+------+--------+-------------+
//|       2| 2053-03-21 16:47:33|  2053-03-21 16:52:21|              3|         0.76|         1|                 N|          33|          66|           2|        5.0|  0.0|    0.5|       0.0|         0.0|                  0.3|         5.8|  Brooklyn|    Brooklyn Heights|  Brooklyn|  DUMBO/Vinegar Hill|      3|   21|   Friday|    16|      47|      3|   21|   Friday|    16|      52|          288|
//|       2| 2018-02-04 07:36:28|  2018-02-04 15:42:57|              1|        17.97|         2|                 N|         132|         162|           1|       52.0|  0.0|    0.5|     10.56|         0.0|                  0.3|       63.36|    Queens|         JFK Airport| Manhattan|        Midtown East|      2|    4|   Sunday|     7|      36|      2|    4|   Sunday|    15|      42|        29189|
//|       2| 2018-02-25 15:50:32|  2018-02-25 15:53:30|              1|         0.34|         1|                 N|         161|         230|           2|        4.0|  0.0|    0.5|       0.0|         0.0|                  0.3|         4.8| Manhattan|      Midtown Center| Manhattan|Times Sq/Theatre ...|      2|   25|   Sunday|    15|      50|      2|   25|   Sunday|    15|      53|          178|
//|       2| 2018-02-25 15:59:25|  2018-02-25 16:06:36|              1|         0.91|         1|                 N|         230|         186|           1|        6.5|  0.0|    0.5|      1.46|         0.0|                  0.3|        8.76| Manhattan|Times Sq/Theatre ...| Manhattan|Penn Station/Madi...|      2|   25|   Sunday|    15|      59|      2|   25|   Sunday|    16|       6|          431|
        
        temp2.createOrReplaceTempView("data")


///////////////////////// END OF PRE PROCESSING DATA
        
        /////// PREPARING CALCULATE TIME
        var TimeRunningNanoSecond:Map[String,Double] = Map()
        
        
        
        
        /////// END OF PREPARING CALCULATING TIME
///////////////////////// START OF BIG DATA ANALYSIS
        
        // ANALYSIS ON TRIPS IN OVERALL
        // OVERALL 
        
            // Most and least distance travelled
                val t0 = System.nanoTime()
            temp2.agg(max(temp2("Trip_distance")),min(temp2("Trip_distance"))).write.format("csv").option("header","true").save(storageDirection+"/result/AOdistancetravelled")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-distancetravelled"->(t1 - t0))

        
        spark.sql("SELECT AVG(Trip_distance) from data")
            // Most and least fare collected
                val t0 = System.nanoTime()
            temp2.agg(max(temp2("total_amount")),min(temp2("total_amount"))).write.format("csv").option("header","true").save(storageDirection+"/result/AOfarecollected")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-farecollected"->(t1 - t0))

            // Most and least time travelled
                val t0 = System.nanoTime()
            temp2.agg(max(temp2("trip_duration")),min(temp2("trip_duration"))).write.format("csv").option("header","true").save(storageDirection+"/result/AOtimetravelled")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-timetravelled"->(t1 - t0))
        
            // Efficiency
                val t0 = System.nanoTime()
            spark.sql("select avg(Trip_distance/trip_duration) from data").write.format("csv").option("header","true").save(storageDirection+"/result/AOefficiency")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-efficiency"->(t1 - t0))

            // Percentage per Vendor
                val t0 = System.nanoTime()
            spark.sql("select VendorID,count(VendorID),count(VendorID)*100/(select count(VendorID) from data) from data group by VendorID").write.format("csv").option("header","true").save(storageDirection+"/result/AOpercentagevendor")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-percentvendor"->(t1 - t0))

            // Average of Passenger per trips
                val t0 = System.nanoTime()
            spark.sql("select avg(passenger_count) from data").write.format("csv").option("header","true").save(storageDirection+"/result/AOpassengertrips")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-avgpassenger"->(t1 - t0))

            // Average of Tips per trips
                val t0 = System.nanoTime()
            spark.sql("select avg(tip_amount) from data").write.format("csv").option("header","true").save(storageDirection+"/result/AOavgtips")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-avgtips"->(t1 - t0))

            // Average of Total Amount per trips
                val t0 = System.nanoTime()
            spark.sql("select avg(total_amount) from data").write.format("csv").option("header","true").save(storageDirection+"/result/AOtotalamount")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AO-avgtotalamount"->(t1 - t0))

        // ANALYSIS BASED ON REGION
        // Not sure if this could be merged with bottom or everything with borough sent here
        
            // Calculating common pickup zone for overall
                val t0 = System.nanoTime()
            spark.sql("select PU_Borough,PU_Zone,count(PU_Zone),PULocationID,count(PULocationID) from data group by PU_Borough,PULocationID,PU_Zone order by count(PULocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ARcommonpickupoverall")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-commonpickupoverall"->(t1 - t0))

            // Calculating common dropoff zone for overall
                val t0 = System.nanoTime()
            spark.sql("select DO_Borough,DO_Zone,count(DO_Zone),DOLocationID,count(DOLocationID) from data group by DO_Borough,DO_Zone,DOLocationID order by count(DOLocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ARcommondropoffoverall")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-commondropoffoverall"->(t1 - t0))
        
            //Question B. Total pickups by time of day by borough
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough,COUNT(PULocationID) as CountPUL, PUHour from data group by PUHour,PU_Borough order by CountPUL desc").write.format("csv").option("header","true").save(storageDirection+"/result/ARquestionbPU")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-questionbPU"->(t1 - t0))
        
            //Question B. Total dropoffs by time of the day by borough
                val t0 = System.nanoTime()
            spark.sql("SELECT DO_Borough,COUNT(DOLocationID) as CountDOL, DOHour from data group by DOHour,DO_Borough order by CountDOL desc").write.format("csv").option("header","true").save(storageDirection+"/result/ARquestionbDO")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-questionbDO"->(t1 - t0))
        
            //Question B. Average pickups by time of day by borough ************ CHECK IF
                val t0 = System.nanoTime()
            spark.sql("select PUHour, avg(CountPUL) from (SELECT PU_Borough,COUNT(PULocationID) as CountPUL, PUHour from data group by PUHour,PU_Borough) group by PUHour order by avg(CountPUL) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ARquestionbAVG")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-questionbAVG"->(t1 - t0))
        
            // Question C. Percentage of pickup per borough 
                val t0 = System.nanoTime()
            spark.sql("select PU_Borough, count(PU_Borough), count(PU_Borough)*100/(select count(PU_Borough) from data) from data group by PU_Borough").write.format("csv").option("header","true").save(storageDirection+"/result/ARquestioncPU")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-questioncPU"->(t1 - t0))
        
            // Question C. Percentage of drop off per borough 
                val t0 = System.nanoTime()
            spark.sql("select DO_Borough, count(DO_Borough), count(DO_Borough)*100/(select count(DO_Borough) from data) from data group by DO_Borough").write.format("csv").option("header","true").save(storageDirection+"/result/ARquestioncDO")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-questioncDO"->(t1 - t0))

            // Question K(Modification). Average number of passengers per day/hour combination by Borough
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough, PUWeekday, PUHour, avg(passenger_count) from data group by PU_Borough, PUWeekday, PUHour order by avg(passenger_count) DESC").write.format("csv").option("header","true").save(storageDirection+"/result/ARquestionkModif")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AR-questionkModif"->(t1 - t0))

        // ANALYSIS BASED ON TIME AND LOCATION
        // TIME
        // NOTE: Not sure if season and night life should be here. I think could be good to delete borough from it, and only highlight in a map 1 for each season and each pickup/dropoff (the most common)
        
            // Question G. Common drop off location per season Fall
                val t0 = System.nanoTime()
            spark.sql("SELECT DO_Borough, DO_Zone, count(DO_Zone), DOLocationID, count(DOLocationID) from data WHERE (DOMonth >= 9 AND DOMonth <=11) group by DO_Borough, DO_Zone, DOLocationID order by count(DOLocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_dropoff_fall")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_DO_fall"->(t1 - t0))

            // Question G. Common drop off location per season Summer
                val t0 = System.nanoTime()
            spark.sql("SELECT DO_Borough, DO_Zone, count(DO_Zone), DOLocationID, count(DOLocationID) from data WHERE (DOMonth >= 6 AND DOMonth <=8) group by DO_Borough, DO_Zone, DOLocationID order by count(DOLocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_dropoff_summer")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_DO_summer"->(t1 - t0))

            // Question G. Common drop off location per season Spring
                val t0 = System.nanoTime()
            spark.sql("SELECT DO_Borough, DO_Zone, count(DO_Zone), DOLocationID, count(DOLocationID) from data WHERE (DOMonth >=3 AND DOMonth <=5) group by DO_Borough, DO_Zone, DOLocationID order by count(DOLocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_dropoff_spring")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_DO_spring"->(t1 - t0))

            // Question G. Common drop off location per season Winter
                val t0 = System.nanoTime()
    spark.sql("SELECT DO_Borough, DO_Zone, count(DO_Zone), DOLocationID, count(DOLocationID) from data WHERE (DOMonth == 12 OR DOMonth == 1 or DOMonth ==2) group by DO_Borough, DO_Zone, DOLocationID order by count(DOLocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_dropoff_winter")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_DO_winter"->(t1 - t0))

            // Guestion G. Common pick up location per season Fall
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough, PU_Zone, count(PU_Zone), PULocationID, count(PULocationID) from data WHERE (PUMonth >= 9 AND PUMonth <=11) group by PU_Borough, PU_Zone, PULocationID order by count(PULocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_pickup_fall")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_PU_fall"->(t1 - t0))

            // Guestion G. Common pick up location per season Summer
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough, PU_Zone, count(PU_Zone), PULocationID, count(PULocationID) from data WHERE (PUMonth >= 6 AND PUMonth <=8) group by PU_Borough, PU_Zone, PULocationID order by count(PULocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_pickup_summer")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_PU_summer"->(t1 - t0))

            // Guestion G. Common pick up location per season Spring   
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough, PU_Zone, count(PU_Zone), PULocationID, count(PULocationID) from data WHERE (PUMonth >=3 AND PUMonth <=5) group by PU_Borough, PU_Zone, PULocationID order by count(PULocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_pickup_spring")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_PU_spring"->(t1 - t0))

            // Guestion G. Common pick up location per season Winter  
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough, PU_Zone, count(PU_Zone), PULocationID, count(PULocationID) from data WHERE (PUMonth == 12 OR PUMonth == 1 or PUMonth ==2) group by PU_Borough, PU_Zone, PULocationID order by count(PULocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon_pickup_winter")  
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questiong_PU_winter"->(t1 - t0))

            // NIGHT LIFE. Common pickup location for weekend night life (after 20 and before 4)
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough, PU_Zone, count(PU_Zone), PULocationID, count(PULocationID) from data WHERE (PUHour >=20 OR PUHour <= 4) AND (PUWeekday == 'Friday' OR PUWeekday == 'Saturday') group by PU_Borough, PU_Zone, PULocationID order by count(PULocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon-pickup-nightlife")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-PUnightlife"->(t1 - t0))

            // NIGHT LIFE. Common dropoff location for weekend night life (after 20 and before 4)
                val t0 = System.nanoTime()
            spark.sql("SELECT DO_Borough, DO_Zone, count(DO_Zone), DOLocationID, count(DOLocationID) from data WHERE (DOHour >=20 OR DOHour <= 4) AND (DOWeekday == 'Friday' OR DOWeekday == 'Saturday') group by DO_Borough, DO_Zone, DOLocationID order by count(DOLocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/ATcommon-dropoff-nightlife")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-DOnightlife"->(t1 - t0))
        
            // Question A. Calculate common pickup location by day of week
                val t0 = System.nanoTime()
            spark.sql("select PU_Borough,PU_Zone,count(PU_Zone),PULocationID,count(PULocationID),PUWeekday from data group by PU_Borough,PU_Zone,PULocationID,PUWeekday order by count(PULocationID) desc").limit(100000).write.format("csv").option("header","true").save(storageDirection+"/result/ATqa_common_pickup_weekday")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questionaPU"->(t1 - t0))

            // Question A. Calculate common dropoff location by day of week
                val t0 = System.nanoTime()
            spark.sql("select DO_Borough,DO_Zone,count(DO_Zone),DOLocationID,count(DOLocationID),DOWeekday from data group by DO_Borough,DO_Zone,DOLocationID,DOWeekday order by count(DOLocationID) desc").limit(100000).write.format("csv").option("header","true").save(storageDirection+"/result/ATqa_common_dropoff_weekday")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questionaDO"->(t1 - t0))

             // Question E. Average number of trips per day of week 
                val t0 = System.nanoTime()
            spark.sql("SELECT PUWeekday, avg(acount) from (select PU_Borough,PUWeekday,count(VendorID) as acount from data group by PU_Borough,PUWeekday) group by PUWeekday order by avg(acount) DESC").write.format("csv").option("header","true").save(storageDirection+"/result/ATquestione")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questione"->(t1 - t0))

            // Question F. Average taxi trips per day by month
                val t0 = System.nanoTime()
            spark.sql("SELECT avg(acount) as AverageTrips, PUDay from (select PU_Borough,PUDay,PUMonth, count(VendorID) as acount from data group by PU_Borough,PUDay, PUMonth) group by PUDay order by PUDay").write.format("csv").option("header","true").save(storageDirection+"/result/ATquestionf")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questionf"->(t1 - t0))

            // Question D. Average trip distance by hour and borough ************** CHECK IF (below)
                val t0 = System.nanoTime()
            spark.sql("SELECT PU_Borough, PUHour, Trip_distance from data group by PU_Borough, PUHour,Trip_distance order by Trip_distance DESC").write.format("csv").option("header","true").save(storageDirection+"/result/ATquestionD_borough_check")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questionD_borough_check"->(t1 - t0))

            // Question D. Average trip distance by hour ************** CHECK IF (above)
                val t0 = System.nanoTime()
            spark.sql("select PUHour,avg(Trip_distance) from (SELECT PU_Borough, PUHour, Trip_distance from data group by PU_Borough, PUHour,Trip_distance) group by PUHour").write.format("csv").option("header","true").save(storageDirection+"/result/ATquestionD_check")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questionD_check"->(t1 - t0))

            // Question H. Total trips per hour
                val t0 = System.nanoTime()
            spark.sql("select PUHour, count(PUHour) from data group by PUHour").write.format("csv").option("header","true").save(storageDirection+"/result/ATquestionh")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questionh"->(t1 - t0))

            // Question I. Total trips per month
                val t0 = System.nanoTime()
            spark.sql("select PUMonth, count(PUMonth) from data group by PUMonth").write.format("csv").option("header","true").save(storageDirection+"/result/ATquestioni")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AT-questioni"->(t1 - t0))
        
        // ANALYSIS BASED ON FARE/PAYMENT
        // MONEY

            // BDFTips. Best day/hour combination for tips
                val t0 = System.nanoTime()
            spark.sql("SELECT PUWeekday, PUHour, avg(tip_amount) from data group by PUWeekday, PUHour order by avg(tip_amount) DESC").write.format("csv").option("header","true").save(storageDirection+"/result/AFbestdaytips")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-bestdaytips"->(t1 - t0))

            // BDFRevenue. Best day/hour combination for fare revenue
                val t0 = System.nanoTime()
            spark.sql("SELECT PUWeekday, PUHour, avg(total_amount) from data group by PUWeekday, PUHour order by avg(total_amount) DESC").write.format("csv").option("header","true").save(storageDirection+"/result/AFbestdayfare")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-bestdayfare"->(t1 - t0))

            // Question J. Most common pickup locations per payment type 
                val t0 = System.nanoTime()
            spark.sql("select PU_Borough,PU_Zone,count(PU_Zone),PULocationID,count(PULocationID),payment_type from data group by PU_Borough,PU_Zone,PULocationID,payment_type order by count(PULocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/AFquestionjPU")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-questionjPU"->(t1 - t0))

            // Question J. Most common drop off locations per payment type 
                val t0 = System.nanoTime()
            spark.sql("select DO_Borough,DO_Zone,count(DO_Zone),DOLocationID,count(DOLocationID),payment_type from data group by DO_Borough,DO_Zone,DOLocationID,payment_type order by count(DOLocationID) desc").write.format("csv").option("header","true").save(storageDirection+"/result/AFquestionjDO")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-questionjDO"->(t1 - t0))
        
            // WhatTimePayment. Most common payment type according to hour
                val t0 = System.nanoTime()
            spark.sql("select payment_type,count(payment_type),PUHour from data group by payment_type,PUHour").write.format("csv").option("header","true").save(storageDirection+"/result/AFwhattimepayment")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-whattimepayment"->(t1 - t0))

            // Question L. Most common payment type per borough
                val t0 = System.nanoTime()
            spark.sql("select PU_Borough, payment_type,count(payment_type) as c_payment from data group by PU_Borough, payment_type order by c_payment desc").write.format("csv").option("header","true").save(storageDirection+"/result/AFquestionl1")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-questionl1"->(t1 - t0))
                val t0 = System.nanoTime()
            spark.sql("select payment_type, count(payment_type) from data group by payment_type").write.format("csv").option("header","true").save(storageDirection+"/result/AFquestionl2")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-questionl2"->(t1 - t0))

            // Average fare revenue per hour (QUESTION M)
                val t0 = System.nanoTime()
            spark.sql("select PUHour, avg(total_amount) from (select PUHour,PUDay,total_amount from data group by PUHour,PUDay,total_amount) group by PUHour").write.format("csv").option("header","true").save(storageDirection+"/result/AFavgfareperhour")
                val t1 = System.nanoTime()
                TimeRunningNanoSecond += ("AF-avgfareperhour"->(t1 - t0))
        
        
        /////////// END OF ANALYSIS
        
        val timeRDD = sc.parallelize(TimeRunningNanoSecond.toSeq)
        timeRDD.saveAsTextFile(storageDirection+"/result/timeelapsed")

    }
}