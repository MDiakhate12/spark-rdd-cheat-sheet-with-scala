> Dataset: https://www.kaggle.com/fedesoriano/heart-failure-prediction/download <br>
> A simple CSV (comma-separated-value) dataset for heart failure prediction from Kaggle <br>

# Dataset preview
| Age | Sex | ChestPainType | RestingBP | Cholesterol | FastingBS | RestingECG | MaxHR | ExerciseAngina | Oldpeak | ST_Slope | HeartDisease |
| --- | --- | ------------- | --------- | ----------- | --------- | ---------- | ----- | -------------- | ------- | -------- | ------------ |
| 40  | M   | ATA           | 140       | 289         | 0         | Normal     | 172   | N              | 0       | Up       | 0            |
| 49  | F   | NAP           | 160       | 180         | 0         | Normal     | 156   | N              | 1       | Flat     | 1            |
| 37  | M   | ATA           | 130       | 283         | 0         | ST         | 98    | N              | 0       | Up       | 0            |
| 48  | F   | ASY           | 138       | 214         | 0         | Normal     | 108   | Y              | 1.5     | Flat     | 1            |
| 54  | M   | NAP           | 150       | 195         | 0         | ST         | 122   | N              | 0       | Up       | 0            |
| 39  | M   | NAP           | 120       | 339         | 0         | ST         | 170   | N              | 0       | Up       | 0            |


# Load Data as RDD
```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession
    .builder()
    .appName("Spark RDD Cheat Sheet with Scala")
    .master("local")
    .getOrCreate()

val rdd = spark.sparkContext.textFile("data/heart.csv")
```

# Map

```scala
val rdd = spark.sparkContext.textFile("data/heart.csv")

rdd
  .map(line => line)
  .collect()
  .foreach(println)
```
# FlatMap
```scala    
rdd
  .flatMap(line => line.split(","))
  .collect()
  .foreach(println)
```

# Map Partitions
```scala    
val collection = spark.sparkContext.parallelize(Array.range(1, 11), 2)

collection
  .mapPartitions(partition => partition)
  .collect()
  .foreach(println)
```
# Map Partitions With Index
```scala    
val collection = spark.sparkContext.parallelize(Array.range(1, 11), 2)

collection
  .mapPartitionsWithIndex((index, partition) =>
    if (index == 0) partition
    else Iterator()
  )
  .collect()
  .foreach(println)
```
# For Each Partitions
```scala    
val collection = spark.sparkContext.parallelize(Array.range(1, 11), 2)

collection.foreachPartition(p => {
  p.toArray.foreach(println)
  println()
})
```
# ReduceByKey

```scala    
// Count Number of M and F
rdd
  .map(line => (line.split(",")(1), 1))
  .reduceByKey((a, b) => a + b)
  .collect()
  .foreach(println)
```
# Filter
```scala    
// Females observations with Normal restingECG having chestPain = ATA 
// columns sex=1, chestPain=2 and restingECG=6

println(
  rdd
    .map(line => {
      val splitted_line = line.split(",")

      (splitted_line(1), splitted_line(2), splitted_line(6))
    })
    .filter(line =>
      line._1 == "F" && line._2 == "ATA" && line._3 == "Normal"
    )
    .count()
)
```

# Sample
```scala    
val rdd_sample = rdd.sample(false, 0.2, 42)

rdd_sample.collect().foreach(println)
println(rdd_sample.count())
println(rdd.count())
```
# Union
```scala    
val sample1 = rdd.sample(withReplacement = false, fraction = 0.3, seed = 42)
val sample2 = rdd.sample(withReplacement = false, fraction = 0.7, seed = 42)

println(f"Sample 1: ${sample1.count()}, Sample 2: ${sample2.count()}")

val union_sample = sample1.union(sample2)

println(f"Union Sample: ${union_sample.count()}")
```
# Intersection 
```scala    
val sample1 = rdd.sample(withReplacement = false, fraction = 0.6, seed = 42)
val sample2 = rdd.sample(withReplacement = false, fraction = 0.6, seed = 41)

println(f"Sample 1: ${sample1.count()}, Sample 2: ${sample2.count()}")

val intersection_rdd = sample1.intersection(sample2)

println(f"Intersection Sample: ${intersection_rdd.count()}")
```
# Distinct
```scala    
val sample1 = rdd.sample(withReplacement = true, fraction = 0.8, seed = 42)
val sample2 = rdd.sample(withReplacement = true, fraction = 0.8, seed = 4)

println(s"Sample 1: ${sample1.count()}, Sample 2: ${sample2.count()}")

val union_sample = sample1.union(sample2)

println(f"Union Sample: ${union_sample.count()}")

val distinct_sample = union_sample.distinct()

println(f"Distint Sample: ${distinct_sample.count()}")

```

# GroupBy
```scala    
rdd
  .map(line => line.split(","))
  .groupBy(line => line(6))
  .map(line => (line._1, line._2.size))
  .collect()
  .foreach(line => println(f"${line._1}: ${line._2}"))
```

# Aggregate
```scala    
val N = rdd.count()
println(N)

def sumAggPartition = (partitionAccumulator: Int, currentValue: Int) =>
  partitionAccumulator + currentValue
def sumAggGlobal = (globalAccumulator: Int, currentValue: Int) =>
  (globalAccumulator + currentValue)

val mean = rdd
  .map(_.split(",")(0))
  .filter(line => !line.contains("Age"))
  .map(line => line.toInt)
  .aggregate(0)(sumAggPartition, sumAggGlobal)
  .toDouble
  ./(N)

println(mean)

val mean2 = rdd
  .map(_.split(",")(0))
  .filter(line => !line.contains("Age"))
  .map(line => line.toInt)
  .reduce((a, b) => a + b)
  .toDouble
  ./(N)

println(mean2)
```
# Aggregate
```scala    
val collection = spark.sparkContext.parallelize(Array.range(1, 11), 2)

println(
  collection.aggregate(0)(
    (acc, value) => {
      println(f"from-seqOp: $acc, $value")
      acc + value
    },
    (endAcc, endValue) => {
      println(f"from-combOp: $endAcc, $endValue")
      endAcc + endValue
    }
  )
)
```
# Sort By
```scala    
rdd
  .map(_.split(",")(0))
  .filter(line => !line.contains("Age"))
  .map(line => line.toInt)
  .sortBy(line => line)
  .collect()
  .foreach(println)
```
# Save As Text File
```scala    
// Add new column "Id"
var i = 0
var newLine = ""
rdd
  .map(line => {
    newLine = f"${if (i == 0) "Id" else i},$line"
    i += 1
    newLine
  })
  .saveAsTextFile("data/heart2")
```
# Join
```scala    
val rdd2 = spark.sparkContext.textFile("data/heart2.csv")

val part1 = rdd2
  .map(_.split(","))
  .map(line =>
    (
      line(0),
      Patient(
        id = line(0),
        sex = line(2),
        chestPainType = line(3),
        restingECG = line(7)
      )
    )
  )

val part2 = rdd2
  .map(_.split(","))
  .map(line =>
    (
      line(0),
      Patient(
        id = line(0),
        age = line(1),
        restingBP = line(4),
        cholesterol = line(5),
        fastingBS = line(6)
      )
    )
  )

part1.take(3).foreach(println)
println()

part2.take(3).foreach(println)
println()

val joinned_part = part1.join(part2)
joinned_part.take(3).foreach(println)
println()
```
# CoGroup VS Join VS Cartesian
```scala    
val part1 =
  spark.sparkContext.parallelize(
    Seq(("A", "Diaf-From-1"), ("A", "Diaf-From-1"), ("B", "Yeah-From-1"))
  )
val part2 =
  spark.sparkContext.parallelize(
    Seq(("A", "Bro-From-2"), ("A", "Walabook-From-2"))
  )

val cogroupped_part = part1.cogroup(part2)
cogroupped_part.collect().foreach(println)
println()

val joinned_part = part1.join(part2)
joinned_part.collect().foreach(println)
println()

val cartesian_part = part1.cartesian(part2)
cartesian_part.collect().foreach(println)
println()
```

# Pipe
```scala    
val collection =
  spark.sparkContext.parallelize(
    Seq(("A", "Diaf-From-1"), ("A", "Diaf-From-1"), ("B", "Yeah-From-1"))
  )

collection.pipe("head -n 5 data/heart2.csv").collect().foreach(println)
```

// Coalesce
```scala
val collectionWithThreePartitions = spark.sparkContext.parallelize(Array.range(1, 1001), 3)

val collectionWithOnePartition = collectionWith3Partitions.coalesce(1)

println(f"Before Coalesce: ${collectionWithThreePartitions.getNumPartitions}")
println(f"After Coalesce: ${collectionWithOnePartition.getNumPartitions}")
```

# Advanced examples
## Create rdd schema from case class

```scala
// Case Class
case class Patient(
  id: String = "",
  age: String = "",
  sex: String = "",
  chestPainType: String = "",
  restingBP: String = "",
  cholesterol: String = "",
  fastingBS: String = "",
  restingECG: String = "",
  maxHR: String = "",
  exerciseAngina: String = "",
  oldpeak: String = ""
)

// Filter with case class fields and pattern matching
print(
  rdd
    .map(line => line.split(","))
    .map(line =>
      Patient(
        sex = Option(line(1)),
        chestPainType = Option(line(2)),
        restingECG = Option(line(6))
      )
    )
    .filter(patient =>
      (
        patient.chestPainType match {
          case Some("ATA") => true
          case _           => false
        }
      ) &&
        (
          patient.restingECG match {
            case Some("Normal") => true
            case _              => false
          }
        ) &&
        (
          patient.sex match {
            case Some("F") => true
            case _         => false
          }
        )
    )
    .count()
)

// A simpler example with filter with case case

println(
  rdd
    .map(_.split(","))
    .map(line =>
      Patient(sex = line(1), chestPainType = line(2), restingECG = line(6))
    )
    .filter(patient =>
      patient.sex == "F" && patient.chestPainType == "ATA" && patient.restingECG == "Normal"
    )
    .count()
)
```