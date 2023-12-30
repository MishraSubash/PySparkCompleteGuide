# General PySpark Guide
###  Some important files and directores in Spark system 
- **bin**: It contains scripts that employ to intract with Spark, including the Spark shells (Spark-SQL, PySpark, Spark-shell, etc)
- **sbin**: Most of the scripts in this directory are administrative in purpose, for starting and stopping Spark components in the cluster in its various deployment modes.
- **kubernetes**: cotains Dockerfiles for creating Docker images for Spark distribution on a Kubernetes cluster. 
- **data**: This directory is populated with *.txt files that serve as input for Spark’s components: MLlib, Structured Streaming, and GraphX.
- **examples**: "how to" code examples and comprehensive documentation.

### Notes: 
   - Spark computations are expressed as operations. These operations are then converted into low-level RDD-based bytecode as tasks, which are distributed to Spark’s executors for execution.
   -  Spark shell that run locally on a laptop, all the operations ran locally, in a single JVM.
   - To exit any of the Spark shells, press Ctrl-D

###  Understanding Spark Application Concepts
- **Application**: A user program built on Spark using its APIs. It consists of a driver program and executors on the cluster
- **SparkSession**: An object that provides a point of entry to interact with underlying Spark functionality and allows programming Spark with its APIs. In an interactive Spark shell, the Spark driver instantiates a SparkSession for you, while in a Spark application, you create a SparkSession object yourself.
- **Job**: A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g., save(), collect()).
- **stage**: Each job gets divided into smaller sets of tasks called stages that depend on each other. Not all Spark operations can happen in a single stage, so they may be divided into multiple stages. Often stages are delineated on the operator’s computation boundaries, where they dictate data transfer among Spark executors.
- **Task**: A single unit of work or execution that will be sent to a Spark executor.

### Transformations, Actions, and Lazy Evaluation
- **Transformation**: as the name suggests, transform a Spark DataFrame into a new DataFrame without altering the original data, giving it the property of immutability. Put another way, an operation such as select() or filter() will not change the original DataFrame; instead, it will return the transformed results of the operation as a new DataFrame.

    **Transformations can be classified as**: 
   - **Narrow Dependencies**:  Any transformation where a single output partition can be computed from a single input partition is a narrow transformation. For example, ```filter()``` and ```contains()``` represent narrow transformations because they can operate on a single partition and produce the resulting output partition without any exchange of data.
   
   - **Wide Dependencies**: For instance,  ```groupBy()``` or ```orderBy()``` instruct Spark to perform wide transformations,
where data from other partitions is read in, combined, and written to disk.  a count (groupBy()) will force a shuffle of data from each of the executor’s partitions across the cluster and  orderBy() requires output from other partitions to compute the final aggregation. 

- **Lazy Evaluation**: All transformations are evaluated lazily. That is, their results are not computed immediately, but they are recorded or remembered as a lineage. A recorded lineage allows Spark, at a later time in its execution plan, to rearrange certain transformations, coalesce them, or optimize transformations into stages for more efficient execution. An action triggers the lazy evaluation of all the recorded transformations.

**A huge advantage of the lazy evaluation scheme is that Spark can inspect your computational query and ascertain how it can optimize it.** 

***How Spark maintains fault tolerance?***
**Ans**: While lazy evaluation allows Spark to optimize your queries by peeking into your chained transformations, lineage and data immutability provide fault tolerance. Because Spark records each transformation in its lineage and the DataFrames are immutable between transformations, it can reproduce its original state by simply replaying the recorded lineage, giving it resiliency in the event of failures.

######### PICTURE ####### page 29 2-1 ###

**Nothing in a query plan is executed until an action is invoked**


## Apache Spark’s Structured APIs
**What is an RDD?**
An RDD (Resilient Distributed Dataset) is the basic abstraction of Spark representing an unchanging set of elements partitioned across cluster nodes, allowing parallel computation. The data structure can contain any Java, Python, Scala, or user-made object.

RDDs offer two types of operations:

- Transformations take an RDD as an input and produce one or multiple RDDs as output.

- Actions take an RDD as an input and produce a performed operation as an output.

The low-level API is a response to the limitations of MapReduce. The result is lower latency for iterative algorithms by several orders of magnitude. This improvement is especially important for machine learning training algorithms.

**Advantages of RDDs**
The advantages and valuable features of using RDDs are:

- **Performance**: Storing data in memory as well as parallel processing makes RDDs efficient and fast.
- **Consistency**: The contents of an RDD are immutable and cannot be modified, providing data stability.
- **Fault tolerance**: RDDs are resilient and can recompute missing or damaged partitions for a complete recovery if a node fails.

**When to use RDD?**
Use an RDDs in situations where:

- **Data is unstructured**. Unstructured data sources such as media or text streams benefit from the performance advantages RDDs offer.
- **Transformations are low-level**. Data manipulation should be fast and straightforward when nearer to the data source.
- **Schema is unimportant**. Since RDDs do not impose schemas, use them when accessing specific data by column or attribute is not relevant.

**What are DataFrame and Dataset?**
A Spark DataFrame is an immutable set of objects organized into columns and distributed across nodes in a cluster. DataFrames are a SparkSQL data abstraction and are similar to relational database tables or Python Pandas DataFrames.

A Dataset is also a SparkSQL structure and represents an extension of the DataFrame API. The Dataset API combines the performance optimization of DataFrames and the convenience of RDDs. Additionally, the API fits better with strongly typed languages. The provided type-safety and an object-oriented programming interface make the Dataset API only available for Java and Scala.

|Feature| Spark RDD | Spark DataFrame | Spark Dataset |
|---------|------------|---------|-----------|
|Data Represenation | Immutable distributed collection of data | Structured data organized into named columns | Distrubited collection of data with optionalschema|
|Data Processing| Fine-grained control | High-level abstration | Ease of use and performance |
| Suitability| Developers who require precise control | Data analysts and SQL experts| Data professional whon need a balance of control and convenience|
|Key distinctions| offers more control, but more comlex| offers more convenience, but less control| offers a balance of control and convenience|
|Language Support | Java, Scala, Python | Jave, Scala, Python, R| Java, Scala|
|Immutable| Yes | Yes| Yes|
|Fault Tolerant |Yes |Yes|Yes|
|Type-safe| Yes| No| Yes|
|Schema |No|Yes| Yes|
|Execution optimization|No| Yes|Yes|
|API Level for Manipulation| Low| high|High|

**Advantages of Dataset**
The key advantages of using Datasets are:

- **Productive**. Compile-time type-safety makes Datasets most productive for developers. The compiler catches most errors. However, non-existing column names in DataFrames detect on runtime.
- **Easy to use**. A rich set of semantics and high-level functions make the Dataset API simple to use.
- **Fast and optimized**. The Catalyst code optimizer provides memory and speed efficiency.

**When to use Datasets?**
Use Datasets in situations where:

- **Data requires a structure**. DataFrames infer a schema on structured and semi-structured data.
- **Transformations are high-level**. If your data requires high-level processing, columnar functions, and SQL queries, use Datasets and DataFrames.
**A high degree of type safety is necessary**. Compile-time type-safety takes full advantage of the speed of development and efficiency.

**Structured APIs in Spark**
*When errors are detected using the Structured API*

| Error | SQL| DataFrames| Datasets|
|--------|-----|--------|-------|
|Syntax Errors| Runtime | Compile Time| Compile Time |
|Analysis Errors| Runtime| Runtime | Compile Time |

**Spark: What’s Underneath an RDD?**
The RDD is the most basic abstraction in Spark. There are three vital characteristics associated with an RDD:
- **Dependencies**: It instructs Spark how an RDD is constructed with its inputs is required. When necessary to reproduce results, Spark can recreate an RDD from these dependencies and replicate operations on it. This characteristic gives RDDs resiliency.
- **Partitions**: partitions provide Spark the ability to split the work to parallelize computation on partitions across executors. In some cases—for example, reading from HDFS—Spark will use locality information to send work to executors close to the data. That way less data is transmitted over the network.
- **Compute function**: It produces an ```Iterator[T]``` for the data that will be stored in the RDD. Spark has no knowledge of the specific data type in T. To Spark it’s an opaque object; it has no idea if you are accessing a column of a certain type within an object. Therefore, all Spark can do is serialize the opaque object as a series of bytes, without using any data compression techniques.

### The DataFrame API
### Page 71
### Basic Python data types in Spark 

| Data type  | Value assigned in Python | API to instantiate   |
| -----------| -------------------------| ---------------------|
|ByteType    | int                      | DataTypes.ByteType   |
|ShortType   | int                      | DataTypes.ShortType  |
|IntegerType | int                      | DataTypes.IntegerType|
|LongType    | int                      | DataTypes.LongType   |
|FloatType   | float                    | DataTypes.FloatType  |
|DoubleType  | float                    | DataTypes.DoubleType |
|StringType  | str                      | DataTypes.StringType |
|BooleanType | bool                     | DataTypes.BooleanType|
|DecimalType | decimal.Decimal          | DecimalType          |

for complex data analytics, when data will be complex, oftern structured or nested, it requires Spark to handle these complex data types. they come in many forms: maps, array, structs, dates, timestamps, fields, etc. 

### Python structured data types in Spark

|Data type | Value assigned in Python |API to instantiate|
|----------|--------------------------|------------------|
|BinaryType| bytearray| BinaryType()|
|TimestampType |datetime.datetime |TimestampType()|
|DateType |datetime.date |DateType()|
|ArrayType |List, tuple, or array |ArrayType(dataType, [nullable])|
|MapType |dict |MapType(keyType, valueType, [nullable])|
|StructType |List or tuple |StructType([fields])
|StructField |A value type corresponding to the type of this field | StructField (name, dataType, [nullable])|

### Spark SQL and the Underlying Engine
**The Catalyst Optimizer**: Spark SQL Execution Plan

Catalyst optimizer takes a computational query and converts it into an execution plan. It goes through four transformational phases. 

- **Phase 1: Analysis**
    SparkSQL engine begins by generating an abstract systax tree(AST) for SQL. It gathers and holds fields, data types, tables, functions, DB, etc and once they've all been resolved, the query proceeds to next phase. 
    
- **Phase 2: Logical Optimization**
    This phase comprises two internal stages. Applying a standardrule based optimization approach, the Catalyst optimizer will first construct a set of multiple plans and then, using its cost-based optimizer (CBO), assign costs to each plan. These plans are laid out as operator trees. They may include, for example, the process of constant folding, predicate pushdown, projection pruning, Boolean expression simplification, etc. This logical plan is the input into the physical plan.

- **Phase 3: Physical Planning**
    In this phase, Spark SQL generates an optimal physical plan for the selected logical plan, using physical operators that match those available in the Spark execution engine.
    
- **Phase 4: Code Generation**
    The final phase of query optimization involves generating efficient Java bytecode to run on each machine. Because Spark SQL can operate on data sets loaded in memory, Spark can use state-of-the-art compiler technology for code generation to speed up execution. In other words, it acts as a compiler. Project Tungsten, which facilitates whole-stage code generation, plays a role here.

## Spark SQL and DataFrames: Introduction to Built-in Data Sources

########### Add More Files #### 

### SQL Tables and Views
Spark allows you to create two types of tables

- Managed Tables:  Spark manages both the metadata and the data in the file store. This could be a local filesystem, HDFS, or an object store such as Amazon S3 or Azure Blob. It means create Table, DB and Schema from within Spark Session using CREATE Statement.
- Unmanaged Table:  Spark only manages the metadata, while you manage the data yourself in an external data source. It means fetching data from external source let's say from 3rd partly SQL using SELECT statement.

Writing a DataFrame to a SQL table is as easy as writing to a file—just use ```saveAsTable()``` instead of ```save()```. This will create a managed table called ```sql_table```
 For example, In Python:
  ```df.write.mode("overwrite").saveAsTable("sql_table"))```

**Some commonly used Windows function in SQL and its equivalent Spark DataFrame API**

|  |SQL|DataFrame API |
|---|----|------|
|Ranking Functions | ```rank()```| ```rank()```|
| |```dense_rank()``` |```denseRank()```|
| |```percent_rank()```| ```percentRank()```|
| |```ntile()``` | ```ntile()```|
| |```row_number()``` | ```rowNumber()``` |
|Analytic functions |```cume_dist()``` |```cumeDist()```|
| |```first_value()```| ```firstValue()```|
| |```last_value()```| ```lastValue()```|
| |```lag()``` |```lag()```|
| |```lead()``` | ```lead()```|

### Structured Streaming 
## Page 232
Stream processing is defined as the continuous processing of endless streams of data.
With the advent of big data, stream processing systems transitioned from single-node
processing engines to multiple-node, distributed processing engines.

**Advantages of DStreams**
- Spark’s agile task scheduling can very quickly and efficiently recover from failures and straggler executors by rescheduling one or more copies of the tasks on any of the other executors.
- The deterministic nature of the tasks ensures that the output data is the same no matter how many times the task is reexecuted. This crucial characteristic enables Spark Streaming to provide end-to-end exactly-once processing guarantees, that is, the generated output results will be such that every input record was processed exactly once.

The DStream API was built upon Spark’s batch RDD API. Therefore, DStreams had the same functional semantics and fault-tolerance model as RDDs. 

## The Fundamentals of a Structured Streaming Query
**Five Steps to Define a Streaming Query**

- **Step 1: Define input sources**
```spark = SparkSession...
lines = (spark
 .readStream.format("socket")
 .option("host", "localhost")
 .option("port", 9999)
 .load())
 ```
 
```readStream.format("socket")``` it is indicating that the data is being read from a TCP socket as a streaming source.

- **Step 2: Transform Data**
DataFrame operations that can be applied on a batch DataFrame can also be applied on a streaming DataFrame. To understand which operations are supported in Structured Streaming, you have to recognize the two broad classes of data transformations:
*Stateless transformations*: 
    Operations like select(), filter(), map(), etc. do not require any information from previous rows to process the next row; each row can be processed by itself. The lack of previous “state” in these operations make them stateless. Stateless operations can be applied to both batch and streaming DataFrames.

*Stateful transformations*:
In contrast, an aggregation operation like count() requires maintaining state to combine data across multiple rows. More specifically, any DataFrame operations involving grouping, joining, or aggregating are stateful transformations. While many of these operations are supported in Structured Streaming, a few combinations of them are not supported because it is either computationally hard or infeasible to compute them in an incremental manner.

- **Step 3: Define output sink and output mode**
After transforming the data, we can define how to write the processed output data with ```DataFrame.writeStream``` (instead of ```DataFrame.write```, used for batch data).
This creates a ```DataStreamWriter``` which, similar to ```DataFrameWriter```, has additional
methods to specify the following:
    - Output writing details (where and how to write the output)
    - Processing details (how to process data and how to recover from failures)
**In Python**
```writer = counts.writeStream.format("console").outputMode("complete")```

**```outputMode```** can take either of ```append``` (new data rows added to the existing Data), ```complete``` (complete write from scratch), and ```update``` (only the rows of the result table/DataFrame that were updated since the last trigger will be output at the end of every trigger. Like SQL update; update the data based on key ID or other identifier. 

- **Step 4: Specify processing details**
Schduling how often the query run and of how to process the data. 
For instance, In Python
```
checkpointDir = "..."
writer2 = (writer
 .trigger(processingTime="1 second")
 .option("checkpointLocation", checkpointDir))
```

**```trigger()``` options**: 
 - **Default**: By default, the streaming query executes data in micro-batches where the next micro-batch is triggered as soon as the previous micro-batch has completed. 
 - **Processing time with trigger interval**: Query will trigger micro-batches at that fixed interval. 
 - **Once**:  Streaming query will execute exactly one micro-batch—it processes all the new data available in a single batch and then stops itself. This is useful when you want to control the triggering and processing from an external scheduler that will restart the query using any custom schedule
- **Continuous**: streaming query will process data continuously instead of in micro-batches. While only a small subset of DataFrame operations allow this mode to be used, it can provide much lower latency (as low as milliseconds) than the micro-batch trigger modes. 
    (*This is an experimental mode as of Spark 3.0*)

**Checkpoint location**: This is a directory in any HDFS-compatible filesystem where a streaming query
saves its progress information—that is, what data has been successfully processed. Upon failure, this metadata is used to restart the failed query exactly where it left off. Therefore, setting this option is necessary for failure recovery with exactly-once guarantees.

- **Step 5: Start the query:** 
Once everything has been specified, the final step is to start the query, which you can do with the following:
In Python: 
    
```streamingQuery = writer2.start()```

Note: start() is a nonblocking method, so it will return as soon as the query has started in the background. If you want the main thread to block until the streaming query has terminated, you can use ```streamingQuery.awaitTermination()```. If the query fails in the background with an error, ```awaitTermination()``` will also fail with that same exception. 

**Putting it all together** 

*This query will count the words of the source data.* 

```
from pyspark.sql.functions import *
spark = SparkSession...
lines = (spark
 .readStream.format("socket")
 .option("host", "localhost")
 .option("port", 9999)
 .load())
words = lines.select(split(col("value"), "\\s").alias("word"))
counts = words.groupBy("word").count()
checkpointDir = "..."
streamingQuery = (counts
 .writeStream
 .format("console")
 .outputMode("complete")
 .trigger(processingTime="1 second")
 .option("checkpointLocation", checkpointDir)
 .start())
streamingQuery.awaitTermination()
```
### Under the Hood of an Active Streaming Query

######### pic 8-9 page 244 ####

*Under the hoold of Structured Streaming, it uses Spark SQL to execute the data. As such, the full power of Spark SQL's hyperoptimized execution engine is utilized to maximize the stream processing throughput, providing key performance advantages.* 

**Recovering from Failures with Exactly-Once Guarantees**
The checkpoint location must not be changed between restarts because this directory contains the unique identity of a streaming query and determines the life cycle of the query. If the checkpoint directory is deleted or the same query is started with a different checkpoint directory, it is like starting a new query from scratch. However, other details like trigger interval, making minor modifications to the transformations between restarts such as filter out corrupted byte data or files can be changed without
breaking fault-tolerance guarantees.

**Health Check of Active Query**
Structured Streaming provides several ways to track the status and processing metrics of an active query.
- **Get current metrics using ```StreamingQuery```**: When a query processes some data in a micro-batch, we consider it to have made some progress. lastProgress() returns information on the last completed micro batch.

```StreamingQuery.lastProgress()```
```
{
 "id" : "ce011fdc-8762-4dcb-84eb-a77333e28109",
 "runId" : "88e2ff94-ede0-45a8-b687-6316fbef529a",
 "name" : "MyQuery",
 "timestamp" : "2016-12-14T18:45:24.873Z",
 "numInputRows" : 10,
 "inputRowsPerSecond" : 120.0,
 "processedRowsPerSecond" : 200.0,
 "durationMs" : {
 "triggerExecution" : 3,
 "getOffset" : 2
 },
 "stateOperators" : [ ],
 "sources" : [ {
 "description" : "KafkaSource[Subscribe[topic-0]]",
 "startOffset" : {
 "topic-0" : {
 "2" : 0,
 "1" : 1,
 "0" : 1
 }
 },
 "endOffset" : {
 "topic-0" : {
 "2" : 0,
 "1" : 134,
 "0" : 534
 }
 },
 "numInputRows" : 10,
 "inputRowsPerSecond" : 120.0,
 "processedRowsPerSecond" : 200.0
  } ],
 "sink" : {
 "description" : "MemorySink"
 }
}
```
```id```: Unique identifier tied to a checkpoint location. This stays the same throughout the lifetime of a query (i.e., across restarts). 

```runId```: Unique identifier for the current (re)started instance of the query. This changes with every restart.

```numInputRows```: Number of input rows that were processed in the last micro-batch.

```inputRowsPerSecond```: Current rate at which input rows are being generated at the source (average over the last micro-batch duration)

```processedRowsPerSecond```: Current rate at which rows are being processed and written out by the sink (aver‐ age over the last micro-batch duration). If this rate is consistently lower than the input rate, then the query is unable to process data as fast as it is being generated by the source. This is a key indicator of the health of the query.

- **Get current status using ```StreamingQuery.status()```**. This provides information on what the background query thread is doing at this moment. For example, printing the returned object will produce something like this:
```
{
 "message" : "Waiting for data to arrive",
 "isDataAvailable" : false,
 "isTriggerActive" : false
}
```
### Options to write data to any storage system
- **Writing Options**: There are two operations that allow you to write the output of a streaming query to arbitrary storage systems: ```foreachBatch()``` and ```foreach()```. They have slightly different use cases: while ```foreach()``` allows custom write logic on every row, ```foreachBatch()``` allows arbitrary operations and custom logic on the output of each microbatch.

- **Write to multiple locations**: If you want to write the output of a streaming query to multiple locations (e.g., an OLAP data warehouse and an OLTP database), then you can simply write the output DataFrame/Dataset multiple times. However, each attempt to write can cause the output data to be recomputed (including possible rereading of the input data). To avoid recomputations, you should cache the ```batchOutputDataFrame```, write it to multiple locations, and then uncache it:

```
def writeCountsToMultipleLocations(updatedCountsDF, batchId):
 updatedCountsDF.persist()
 updatedCountsDF.write.format(...).save() # Location 1
 updatedCountsDF.write.format(...).save() # Location 2
 updatedCountsDF.unpersist()
Frames.unt())
```

## Incremental Execution and Streaming State
Structured Streaming can incrementally execute most DataFrame aggregation operations. You can aggregate data by keys (e.g., streaming word count) and/or by time (e.g., count records received every hour)

### Aggregations Not Based on Time
Aggregations not involving time can be broadly classified into two categories:

- Global aggregations: Aggregations across all the data in the stream. For example, say you have a stream of sensor readings as a streaming DataFrame named sensorReadings. You can calculate the running count of the total number of readings received with the following query:
```
runningCount = sensorReadings.groupBy().count()
```
***You cannot use direct aggregation operations like ```DataFrame.count()``` and ```Dataset.reduce()``` on streaming DataFrames. This is because, for static DataFrames, these operations immediately return the final computed aggregates, whereas for streaming DataFrames the aggregates have to be continuously updated. Therefore, you have to always use ```DataFrame.groupBy()``` or ```Dataset.groupByKey()```
for aggregations on streaming DataFrames.***

- Grouped aggregations: Aggregations within each group or key present in the data stream.
```baselineValues = sensorReadings.groupBy("sensorId").mean("value")```

*All built-in aggregation functions*
- ```sum()```
- ```mean()```
- ```stdev()```
- ```countDistinct()```
- ```collect_set()```
- ```approx_count_distinct()```

mean("value"))s))es())s)))




