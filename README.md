# nyc_surge_pricing_estimation
Estimation surge pricing and traffic congestion using Kafka,Hadoop,Spark-SQL,Spark-Streaming,My-SQL
    Visualization using Qlicksense

# Use case details

We would like to build a surge pricing mechanism and the price calculation will be dependent on the
supplydemand ratio at the specific location.

    ● We define location as a geohash level 6 grid measuring 600M X 1200M
 
    ● The supply and demand ratio will be an input variable to the formula to calculate the appropriate
    surge pricing multiplier in that location.
    
    ● On the demand side the demand measurement is based on incoming booking requests within the
      geohash (the booking request will have a lat lon attached, geohash has to be calculated based on
      the latlon)

    ● On the supply side the measurement is based on drivers that are within that geohash (drivers report
      their locations periodically (assume every 5 seconds) with their current latitude and longitude
      position)

We will arrive at the supply demand ratio using the counts of each stream.

Using the same dataset we would also like you to do something to estimate the traffic congestion in each
geohash at any point of time using factors like travel time and trip distance.

We would like to have two versions each of the supply demand ratio calculator and traffic congestion
estimator

    ● a realtime version that does aggregations on the two streams every 10 minutes to come up with the
       supply/demand ratios and traffic congestion estimates for each geohash

    ● and a batch version which will allow us to look back at historical supply and demand over the last
       few weeks.

You can use the data available from http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml as the
sample dataset to generate the appriopriate datasets or data streams for completing the exercise.

For the final part of the exercise, we would like you to include hourly weather data to your data set so that
we can see analysis and visualizations of how the supply,demand and congestion is affected by the
weather.

You can find the historical weather data here

https://www.wunderground.com/history/airport/KJFK/2014/1/2/DailyHistory.html?MR=1


# Contents
 1. Introduction of use case
 2. NYC Dataset - Dimensions
 3. Approach for Surge-pricing estimation
 4. Approach for traffic congestion estimation
 5. Technical Process flow diagram
 6. Surge-pricing estimation results
 7. Traffic congestion estimation results
 8. Weather effect on surge pricing and traffic congestion

Above details were provide in below file in the same repository

https://github.com/amithgitdas/nyc_surge_pricing_estimation/blob/master/README.md.pdf

below are code repos for above usecase

Batch version : https://github.com/amithgitdas/nyc_surge_pricing_estimation/tree/master/nyc_surgepricing_calculation

Streaming version : https://github.com/amithgitdas/nyc_surge_pricing_estimation/tree/master/nyc_surgepricing_streaming

Kafka producer : https://github.com/amithgitdas/nyc_surge_pricing_estimation/tree/master/surgepricing_kafkaproducer



