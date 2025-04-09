Project 3 Analysing Flight Interconnected Data

![TartuLogo](../../images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Alejandro Ballesteros Perez, Phasha Davrishev, Roman Krutsko, Nika Mgaloblishvili

## License
This project is licensed under the [Apache License 2.0](LICENSE).

## Introduction

`Fill me`

## Queries

### Query 1 | Compute different statistics

We calculated different statistics for flights to analyze flights data.
Out-degree, which is the number of flights leaving the airport is calculated using this formula:

```python
out_degree = edges.groupBy("src").agg(count("*").alias("out_degree")) \
                  .withColumnRenamed("src", "id")
```

In-degree is the number of flights arriving at each airport, is calculated using this formula:

```python
in_degree = edges.groupBy("dst").agg(count("*").alias("in_degree")) \
                 .withColumnRenamed("dst", "id")
```

Total degree is combined, formula:

```python
degree_df = vertices.join(in_degree, on="id", how="left") \
                    .join(out_degree, on="id", how="left") \
                    .na.fill(0) \
                    .withColumn("total_degree", col("in_degree") + col("out_degree"))
```

Triangle count is number of sets of three vertices, where each vertex is connected to the other two by edges,
forming a cycle of length three. In case of my implementation
the algorithm first normalizes the edges, collects neighbors for each vertex, joins these neighbor sets based on
adjacent edges to find candidate vertex pairs, and then counts the number of intersections to determine the triangles
each vertex is part of.
The GraphFrames implementation deduplicates edges and uses its built-in motif-finding functionality to directly locate
triangle patterns. Then it aggregates the results by exploding the found vertices and grouping to count each vertex's
participation. 
Our implementation manually normalizes edges, builds neighbor sets, and identifies triangles by joining these sets and
computing their intersections, that is why there is a difference in some counts.

### Query 2 | Total number of triangles in the graph

To count total number of triangles in graph, we have to combine the results of oll airports using this formula:

```python
total_triangles = triangle_counts.agg(F.sum("triangleCount").alias("total")).collect()[0]["total"]
```

As we can see, the result of total triangles from GraphFrames has to be divided by 3, as it accounted for three times
when aggregating vertex counts. So comparing the total count of triangles calculated by our implementation and built-in
GraphFrames `graph.triangleCount()`, we can see that count is same, which is `15991`.
