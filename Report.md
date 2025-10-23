# Assignment 3 Report

## Team Members

Please list the members here

## Responses to questions posed in the assignment

_Note:_ Include the Spark execution history for each task. Name the zip file as `assignment-3-task-<x>-history.zip`.

### Task 1: Word counting

1. If you were given an additional requirement of excluding certain words (for example, conjunctions), at which step you would do this and why? (0.1 pt)

Answer: We would remove stop words immediately after the FlatMap step, as each word is available individually here. This avoids unnecessary key-value pairs and shuffles, which saves computing time.


2. In Lecture 1, the potential of optimizing the mapping step through combined mapping and reduce was discussed. How would you use this in this task? (in your answer you can either provide a description or a pseudo code). Optional: Implement this optimization and observe the effect on performance (i.e., time taken for completion). (0.1 pt)

Answer: Spark combines mapping and reduction using the combiner function (reduceByKey or combineByKey). This performs local aggregation per partition before results are shuffled. This reduces the shuffle volume.

3. In local execution mode (i.e. standalone mode), change the number of cores that is allocated by the master (.setMaster("local[<n>]") and measure the time it takes for the applicationto complete in each case. For each value of core allocation, run the experiment 5 times (to rule out large variances). Plot a graph showing the time taken for completion (with standard deviation) vs the number of cores allocated. Interpret and explain the results briefly in few sentences. (0.4 pt)

Answer: The runtime decreases from 1 core (≈ 1.57 s) to about 1.45 s with 2 cores, but then remains almost constant.
The small difference shows that the data set is small and that the parallelization overheads (e.g., task scheduling, thread management) compensate for the potential speed gain.
Spark therefore does not scale linearly with the number of cores in this experiment—the effect would be more noticeable with larger data sets.

4. Examine the execution history. Explain your observations regarding the planning of jobs, stages, and tasks. (0.4 pt)

The Spark History Server shows that the **WordCount** application ran successfully in cluster mode.  
It completed **one job** (`Job 0`) in about **6 seconds**, using two stages and a total of **six tasks**.

#### Job and Stage Structure
- **Job 0:** Triggered by the action `collect()` at *TaskWordCounting.java:95*.
- The job consists of **two stages**, separated by a **shuffle boundary**:
    - **Stage 0 – Map stage:**  
      Executes `textFile`, `flatMap`, and `mapToPair` transformations  
      (see DAG Visualization – `TaskWordCounting.java:64–79`).  
      It reads the dataset from HDFS and generates `(word, 1)` pairs.  
      → Duration ≈ 2 s, 3 tasks succeeded.
    - **Stage 1 – Reduce stage:**  
      Executes `reduceByKey` (*TaskWordCounting.java:87*).  
      Spark groups identical keys via a shuffle operation  
      (≈ 5.7 KiB shuffle write/read) and sums word counts.  
      → Duration ≈ 0.4 s, 3 tasks succeeded.

#### Executors and Tasks
Three executors were active (`driver`, `executor 0`, `executor 1`),  
and each stage executed three parallel tasks, corresponding to the dataset’s partitions.

#### DAG and Dependency Visualization
The DAG Visualization shows a clear linear dependency:

textFile → flatMap → mapToPair → reduceByKey → collect

Stage 0 (map) feeds its output into Stage 1 (reduce) through a shuffle edge,  
confirming Spark’s separation between **narrow** (map) and **wide** (reduce) transformations.

#### Summary
Spark planned:
- **1 Job**
- **2 Stages (map + reduce)**
- **6 Tasks total**
- **Shuffle Size ≈ 5.7 KiB**

This demonstrates Spark’s typical MapReduce execution model:  
Stage 0 performs record transformation, Stage 1 performs aggregation after shuffle.  
The small shuffle size and short total duration (≈ 7 s) indicate that the dataset was small and the cluster processed it efficiently.

### Task 2

1. For each of the above computation, analyze the execution history and describe the key stages and tasks that were involved. In particular, identify where data shuffling occurred and explain why. (0.5pt)
Each of the three computations (hourly averages, month-wise increase/decrease, and correlations) triggered a separate Spark job consisting of multiple stages.
In all aggregation jobs, Spark executed a typical pipeline:
   Scan CSV → WholeStageCodegen → Exchange → HashAggregate

The Exchange step marks where data shuffling occurred — Spark redistributed records across partitions so that all rows with the same grouping key (e.g., month) were processed on the same executor.
This was required for the groupBy and agg operations in both the hourly-average and max/min-deΩlta computations.

During the correlation computations (corr at TaskRoomSensorTelemetry.java:107), Spark also performed shuffles, visible through AQEShuffleRead nodes in the DAG visualization.
These occurred because statistical functions such as stat.corr require data from multiple partitions to be combined before computing global correlations.


2. You had to manually partition the data. Why was this essential? Which feature of the dataset did you use to partition and why?(0.5pt)
   
Manual partitioning was essential to optimize performance and reduce shuffle overhead.
By explicitly partitioning the dataset based on temporal attributes—primarily thΩe month column—Spark ensured that all records from the same month were placed within the same partition.
This allowed each executor to compute intermediate aggregates (e.g., hourly averages or monthly deltas) locally, minimizing cross-partition data transfers.
Without this partitioning, Spark would have needed to shuffle much larger portions of the dataset for every groupBy operation, resulting in higher latency and inefficient resource usage.

3. Optional: Notice that in the already provided pre-processing (in the class DatasetHelper), the long form of timeseries data, i.e., with a column _field that contained values like temperature etc., has been converted to wide form, i.e. individual column for each measurement kind through and operation called pivoting. Analyze the execution log and describe why this happens to be an expensive transformation.

### Task 3

1. Explain how the K-Means program you have implemented, specifically the centroid estimation and recalculation, is parallelized by Spark (0.5pt)

The K-Means algorithm was parallelized using Spark’s RDD model.
In each iteration, data points were assigned to the nearest centroid using a parallel mapToPair() operation (E-step).
Centroids were then recalculated with groupByKey() and mapValues() (M-step), which triggered a shuffle to aggregate points per cluster across partitions.
The current centroids were broadcast to all executors, allowing each node to compute locally without redundant data transfers.
Thus, Spark parallelized both assignment and centroid recomputation across executors, performing one distributed job per iteration.

## Declarations (if any)
