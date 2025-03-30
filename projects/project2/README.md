Project 2 Analyzing New York City Taxi Data

![TartuLogo](../../images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Alejandro Ballesteros Perez, Phasha Davrishev, Roman Krutsko, Nika Mgaloblishvili

## License
This project is licensed under the [Apache License 2.0](LICENSE).

## Introduction  
This report analyzes taxi trip data in New York City using a sample dataset provided on http://www.debs2015.org/call-grand-challenge.html. Each row in the dataset represents a single taxi ride, containing attributes such as:  

- A unique identifier for the taxi (hashed license number)  
- Pickup and drop-off locations (longitude/latitude)  
- Pickup and drop-off timestamps  
- Total trip distance
- Total fare amount

## Objectives
This report focuses on two metrics related to taxi utilization and trip patterns:
1. **Most Frequent Routes** - Identify the 10 most frequent taxi routes during the 30-minute time window.
2. **Most Profitable Areas** - Identify the 10 most profitable areas in New York City based on the profitability metrics. Profitability of an area is defined as the median fare + tip for trips that started in the area and ended within the last 15 minutes divided by the number of empty taxes in the area.


## Queries

### Query 1 | Most Frequent Routes
This query identifies the 10 most frequent taxi routes during a 30-minute time window. A route is defined as a pair of pickup and drop-off cells on a 500mx500m grid.

The cell IDs are calculated using the following formula:
```python
cell_x = floor((pickup_longitude - grid_origin_longitude) / delta_longitude) + 1
cell_y = floor((grid_origin_latitude - pickup_latitude) / delta_latitude) + 1
```
where `grid_origin_longitude` and `grid_origin_latitude` are the coordinates of cell 1.1, while the `delta_longitude` and `delta_latitude` are the approximate degrees for 500m changes in longitude and latitude, respectively.

Afterwards, the top 10 most frequent routes are calculated using the groupBy function
```python
df_frequent_routes = df_last30.groupBy("start_cell", "end_cell").count().withColumnRenamed("count", "Number_of_Rides")
```

For the part 2 of the task 1, the trip that triggered the recalculation of the route is also included in the output. The trigger row is defined as follows
```python
trigger_row = df_last30.orderBy(col("dropoff_datetime").desc()).limit(1).collect()[0]
```
which is the row with the latest dropoff time.

### Query 2 | Most Profitable Areas
This query identifies the 10 most profitable areas in New York City based on the profitability metrics. Profit originating from an area is defined as the median fare + tip for trips that started in the area and ended within the last 15 minutes divided by the number of empty taxis in the area.

Firstly the pickup and dropoff cells are calculated using the same formula as in Query 1. 

Afterwards, the profit is calculated using the following formula:
```python
df_with_cells.filter(col("dropoff_datetime") >= F.lit(ref_time_profit)) \
                             .withColumn("profit", col("fare_amount") + col("tip_amount"))
```
where `ref_time_profit` is the reference time for the last 15 minutes.
The profit is then grouped by the pickup cell and the median is calculated using the `agg` function:
```python
df_profit = df_profit.groupBy("pickup_cell").agg(F.expr("percentile_approx(profit, 0.5)").alias("median_profit"))
```

Finally, the number of empty taxis in the area (the sum of taxis that had a drop-off location in that area less than 30 minutes ago and had no following pickup yet) is calculated.
For that the medallions without the pickup are identified using the window function and grouped by the pickup cell:
```python
df_with_next = df_with_cells.withColumn("next_pickup", F.lead("pickup_datetime").over(windowSpec))
    df_empty = df_with_next.filter(
        (col("dropoff_datetime") >= F.lit(ref_time_empty)) &
        ((col("next_pickup").isNull()) | (col("next_pickup") > col("dropoff_datetime")))
    )

df_empty_grouped = df_empty.groupBy("dropoff_cell") \
                               .agg(F.countDistinct("medallion").alias("empty_taxi_count")) \
                               .withColumnRenamed("dropoff_cell", "cell_id")
```

Afterwards this data is joined with the profit data to get the final result:
```python
df_profit_grouped.join(df_empty_grouped, on="cell_id", how="outer") \
                               .withColumn(
                                   "profitability_of_cell",
                                   F.when(col("empty_taxi_count") > 0, col("median_profit") / col("empty_taxi_count")).otherwise(None)
                               )
```


