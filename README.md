### DPSPC: A Density Peak-based Statistical Parallel Clustering Algorithm for Big Data

Code Language: Scala

Distributed Computing Platform: Spark

Main Program: `DPSPC.scala`

#### Usage:

Compile and package the code to get `dpspc.jar`:

```shell
cd dpspc
sbt package
```

Run clustering algorithm:

```shell
scala -classpath dpspc.jar distribute.DPSPC [args]
```

Parameters (args):

```scala
val inPath: String, // input path
val outPath: String, // output path
val dim: Int, // dimension of data
val numPart: Int, // number of partitons
val sampleSize: Int = 3, // number of samples to be computed (option)
val pct: Double = 0.02, // percentage for computing cutoff distance (option)
val windowSize: Int = 20, // window size for finding density peaks (option)
val theta: Double = 2e-5, // variance threshold for finding density peaks (option)
val numOfRep: Int = 5 // number of representative points of each cluster (option)
```

Example:

```shell
scala -classpath dpspc.jar distribute.DPSPC data/worms_norm.txt data/worms_result_20_2 2 20 2 0.02 10 2e-5 5
```

The data in file `data/worms_norm.txt ` will be processed, and the clustering results will be output to the file `data/worms_result_20_2`.

The  code also supports clustering of large datasets distributed stored across multiple machines.

#### Test:

`TestDataSkew`: Test for data skew. Calculate the amount of data for each partition and the number of clusters.

`TestSample`: Test for effect of sampling. Test the running time and accuracy under different sampling rates.

`TestNeighborSearch`: Test for range neighbor search using KD-Tree.