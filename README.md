# Bringing Hadoop Input/Output Format Support to PySpark

Two powerful features of Apache Spark include its native APIs provided in Scala, Java and Python, and its compatability with any Hadoop-based input or output source. This language support means that users can quickly become proficient in the use of Spark even without experience in Scala, and furthermore can leverage the extensive set of third-party libraries available (for example, the many data analysis libraries for Python).

Built-in Hadoop support means that Spark can work "out the box" with any data storage system or format that implements Hadoop's `InputFormat` and `OutputFormat` interfaces for reading and writing data, including HDFS, HBase, Cassandra, Elasticsearch, DynamoDB and many others, as well as various data serialization formats such as SequenceFiles, Parquet, Avro, Thrift and Protocol Buffers.

Previously, Hadoop InputFormat/OutputFormat support was provided only in Scala or Java. To access such data sources in Python, other than simple text files, users would need to first read the data in Scala or Java, and write it out as a text file for reading again in Python. With the release of Spark 1.1, Python users can now read and write their data directly from and to any Hadoop-compatible data source. 

## An Example: Reading SequenceFiles

[SequenceFile](http://wiki.apache.org/hadoop/SequenceFile) is the standard binary serialization format for Hadoop. It stores records of `Writable` key-value pairs, and supports splitting and compression. SequenceFiles are a commonly used format in particular for intermediate data storage in Map/Reduce pipelines, since they are more efficient than text files.

Spark has long supported reading SequenceFiles natively using the `sequenceFile` method available on a `SparkContext` instance, which also utilizes Scala features to allow specifying the key and value type in the method call parameters. For example, to read a SequenceFile with `Text` keys and `DoubleWritable` values in Scala, we would do the following:

```
val rdd = sc.sequenceFile[String, Double](path)
```

Spark takes care of converting `Text` to `String` and `DoubleWritable` to `Double` for us automatically.

The new PySpark API functionality exposes a `sequenceFile` method on a Python `SparkContext` instance that works in much the same way, with the key and value types being inferred by default. The `saveAsSequenceFile` method available on a PySpark `RDD` allows users to save an `RDD` of key-value pairs as a SequenceFile. For example, we can create an `RDD` from a Python collection, save it as a SequenceFile, and read it back using the following code snippet:

```
rdd = sc.parallelize([('key1', 1.0), ('key2', 2.0), ('key3', 3.0), ('key4', 4.0)])
rdd.saveAsSequenceFile('/tmp/pysequencefile/')
...
sc.sequenceFile('/tmp/pysequencefile/').collect()
[(u'key1', 1.0), (u'key2', 2.0), (u'key3', 3.0), (u'key4', 4.0)]
```

## Under the Hood

This feature is built on top of the existing Scala/Java API methods. For it to work in Python, there needs to be a bridge that converts Java objects produced by Hadoop```InputFormats``` to something that can be serialized into pickled Python objects usable by PySpark (and vice versa).

For this purpose, a ```Converter``` trait is introduced, along with a pair of default implementations that handle the standard Hadoop [Writables](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/Writable.html).

## Custom Hadoop Converters for PySpark

While the default converters handle the most common Writable types, users need to supply custom converters for custom Writables, or for serialization frameworks that do not produce Writables. To see an illustration of this, some additional converters for [HBase](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/pythonconverters/HBaseConverters.scala) and [Cassandra](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/pythonconverters/CassandraConverters.scala), together with related [PySpark scripts](https://github.com/apache/spark/tree/master/examples/src/main/python), are included in the Spark 1.1 example sub-project.

## A More Detailed Example: Custom Converters for Avro

For those who want to dive deeper, we will show how to write more complex custom PySpark converters, using the [Apache Avro](http://avro.apache.org/docs/current/) serialization format as an example.

One thing to consider is what input data the converter will be getting. In our case, we intend to 
use the converter with ```AvroKeyInputFormat``` and the input data will be Avro records wrapped in an AvroKey. 
Since we want to work with all 3 Avro data mappings (Generic, Specific and Reflect), for each Avro schema type,
we need to handle all possible data types produced by those mappings. For example, for Avro ```BYTES``` type, 
both Generic and Specific mappings output ```java.nio.ByteBuffer```, while Reflect mapping outputs 
```Array[Byte]```. So our ```unpackBytes``` method needs to handle both cases. 

```
  def unpackBytes(obj: Any): Array[Byte] = {
    val bytes: Array[Byte] = obj match {
      case buf: java.nio.ByteBuffer => buf.array()
      case arr: Array[Byte] => arr
      case other => throw new SparkException(
        s"Unknown BYTES type ${other.getClass.getName}")
    }
    val bytearray = new Array[Byte](bytes.length)
    System.arraycopy(bytes, 0, bytearray, 0, bytes.length)
    bytearray
  }
```

Another thing to consider is what data types the converter will output, or equivalently, what data types 
PySpark will see. For example, for the Avro ```ARRAY``` type, the Reflect mapping may produce primitive arrays,
Object arrays or ```java.util.Collection``` depending on its input. We convert all of these to ```java.util.Collection```, which are in turn serialized into instances of a Python ```List```.

```
  def unpackArray(obj: Any, schema: Schema): java.util.Collection[Any] = obj match {
    case c: JCollection[_] =>
      c.map(fromAvro(_, schema.getElementType))
    case arr: Array[_] if arr.getClass.getComponentType.isPrimitive =>
      arr.toSeq
    case arr: Array[_] =>
      arr.map(fromAvro(_, schema.getElementType)).toSeq
    case other => throw new SparkException(
      s"Unknown ARRAY type ${other.getClass.getName}")
  }
```

Finally, we need to handle nested data structures. This is done by recursively calling between individual 
```unpack*``` methods and the central switch ```fromAvro```, which handles the dispatching for all Avro schema types.


```
  def fromAvro(obj: Any, schema: Schema): Any = {
    if (obj == null) {
      return null
    }
    schema.getType match {
      case UNION   => unpackUnion(obj, schema)
      case ARRAY   => unpackArray(obj, schema)
      case FIXED   => unpackFixed(obj, schema)
      case MAP     => unpackMap(obj, schema)
      case BYTES   => unpackBytes(obj)
      case RECORD  => unpackRecord(obj)
      case STRING  => obj.toString
      case ENUM    => obj.toString
      case NULL    => obj
      case BOOLEAN => obj
      case DOUBLE  => obj
      case FLOAT   => obj
      case INT     => obj
      case LONG    => obj
      case other   => throw new SparkException(
        s"Unknown Avro schema type ${other.getName}")
    }
  }
```

The complete source code for ```AvroWrapperToJavaConverter``` can be found in the Spark examples, in [AvroConverters.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/pythonconverters/AvroConverters.scala), while the related PySpark script for using the converter can be found [here](https://github.com/apache/spark/blob/master/examples/src/main/python/avro_inputformat.py).

## Conclusion and Future Work

We're excited to bring this new feature to PySpark in the 1.1 release, and look forward to seeing how users make use of both the built-in functionality, and custom converters.

One limitation of the current ```Converter``` interface is that there is no way to set custom configuration options. A future improvement could be to allow converters to take a 
Hadoop [Configuration](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/conf/Configuration.html) that would allow configuration at runtime.
