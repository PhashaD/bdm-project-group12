Project 3 Analysing Flight Interconnected Data

![TartuLogo](../../images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Alejandro Ballesteros Perez, Phasha Davrishev, Roman Krutsko, Nika Mgaloblishvili

## License
This project is licensed under the [Apache License 2.0](LICENSE).

## Introduction
This report analyzes flight data in the USA, by representing the data as a graph. Each airport is represented as a vertex, and each flight is represented as an edge between two vertices. The goal is to analyze the graph structure and compute various statistics, such as in-degree, out-degree, triangle counts, centrality measures, and PageRank.

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
forming a cycle of length three. In case of my implementation,
the algorithm first normalizes the edges, collects neighbors for each vertex, joins these neighbor sets based on
adjacent edges to find candidate vertex pairs, and then counts the number of intersections to determine the triangles
each vertex is part of.
The GraphFrames implementation deduplicates edges and uses its built-in motif-finding functionality to directly locate
triangle patterns. Then it aggregates the results by exploding the found vertices and grouping to count each vertex's
participation. 
Our implementation manually normalizes edges, builds neighbor sets, and identifies triangles by joining these sets and
computing their intersections, that is why there is a difference in some counts.

### Query 2 | Total number of triangles in the graph

To count total number of triangles in graph, we have to combine the results of all airports using this formula:

```python
total_triangles = triangle_counts.agg(F.sum("triangleCount").alias("total")).collect()[0]["total"]
```

As we can see, the result of total triangles from GraphFrames has to be divided by 3, as it accounted for three times
when aggregating vertex counts. So comparing the total count of triangles calculated by our implementation and built-in
GraphFrames `graph.triangleCount()`, we can see that count is same, which is `15991`.


### Query 4  | PageRank
This query calculates the PageRank of each vertex in the graph. PageRank mesures the importance of each vertex based on the number of vertices connected to it and the PageRank of those vertices. The formula used for PageRank calculation is:

![PageRank Formula](../../images/project3/pageRankFormula.png)

where d is the damping factor, which is set to 0.85 in our implementation. Damping factor represents a probability that a user will continue clicking on links, rather than starting a new random search. 
PageRank is an iterative algorithm, and the number of iterations is set to 10 in our implementation. 

In the implementation, PageRank of each vertex is initialized to 1.0. Afterwards, the contribution of each vertex to the PageRank of its neighbors is calculated, in the following way:

```python
(edges.join(ranks, edges.src == ranks.id)
                     .join(out_degree, edges.src == out_degree.id) # get all the vertices and number of outgoing edges
                     .select(
                         edges.dst.alias("id"),
                         (ranks["rank"] / out_degree["out_degree"]).alias("contrib") # contribution the destination vertex receives
                     ))
```
as can be seen, the contribution each vertex gives to its neighbors is calculated by dividing its PageRank by the number of outgoing edges. 

Then, the PageRank of each vertex is calculated by summing the contributions from all its neighbors and applying the damping factor:

```python
grouped_contributions = (contributions.groupBy("id")
              .agg(F.sum("contrib").alias("sum_contrib"))
              .select(
                  col("id"),
                  (((1 - damping) / N) + damping * col("sum_contrib")).alias("rank") # apply pagerank formula
              ))
```

Given the fact that not all vertices may have received contributions, a rank of (1 - damping) / N is given to such vertices, where N is the number of vertices in the graph:
```python
dangling = vertices.join(grouped_contributions.select("id"), "id", "left_anti") \
                       .withColumn("rank", F.lit((1 - damping) / N))
```

This process is repeated for 10 iterations, and the final PageRank of each vertex is obtained. The results are sorted in descending order to identify the most important vertices in the graph.
```python
ranks.orderBy("rank", ascending=False).show()
```

Afterwards the results are visualized using the networkx and matplotlib libraries. The vertices are represented as circles, and the size of each circle is proportional to the PageRank of the corresponding vertex. The graph is displayed using a spring_layout, which positions nodes using Fruchterman-Reingold force-directed algorithm

![PageRank Graph](../../images/project3/pagerank.png)

### Query 5 | Finding the Most Connected Airports Using Direct Flight Routes

To identify the most connected airports, we analyzed direct flight connections by treating each flight as a directed edge from ORIGIN to DEST. The goal was to compute how many unique airports each airport directly connects with (either as a departure or arrival point).

1. Selected distinct (ORIGIN, DEST) pairs from the dataset.
2. Created bidirectional edges by unifying both (ORIGIN → DEST) and (DEST → ORIGIN) to account for connections in both directions.
3. Grouped by each airport and counted the number of distinct connected airports using countDistinct.
4. Sorted the airports in descending order of connection counts.

This provided a ranked list of airports based on the number of unique direct connections they have. These represent the most connected nodes in the flight network and could indicate hub airports.

|airport|num_connections|
|-------|---------------|
|    ATL|            165|
|    ORD|            141|
|    DFW|            134|
|    DTW|            124|
|    MSP|            119|
|    ...|            ...|


### Visualization | Airport Connectivity Network

To visualize the network of airport connections, we exported the flight route data into a format compatible with NetworkX and used Matplotlib for visualization.

1. Converted the unique (ORIGIN, DEST) pairs from PySpark into a list of edges.

2. Created a directed graph using networkx.DiGraph() and added the route edges.

3. Calculated node sizes based on degree to emphasize the most connected airports.

4. Used spring_layout to lay out nodes in a visually pleasing manner.

5. Plotted nodes, edges, and labels with matplotlib.pyplot.

The resulting graph illustrates how airports are interconnected across the network. Highly connected hubs appear with larger nodes, and the edges show direct flight paths. This helps to visually identify central nodes in the flight network and observe the structural flow of connectivity.


![Airport Connectivity](../../images/project3/connectivity_network.jpg)


