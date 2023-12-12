# *PySpark Complete Guide*
## Practicing PySpark With Glue
- Step 1: Use CloudFormation template to create AWS resources to complete this practice. CF template will create S3 Bucket, Glue Tables, IAM roles with necessary permissions.
- Step 2: Go to AWS Glue and create Glue Notebook and start executing PySpark code
  
Note: In order to start practicing this exmaple, order and customer csv files should be uploaded to the respective orders and customers s3 bucket created by CloudFormation Template.

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
