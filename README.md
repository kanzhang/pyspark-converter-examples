---
---

# Writing custom Hadoop input/output converters for PySpark

In Spark 1.1 release, PySpark API is enhanced with a set of methods to read/write Hadoop data files or stores, 
using suitable Hadoop ```InputFormats```/```OutputFormats```. This feature is built on top of existing 
Scala/Java API methods. For
it to work in Python environment, there needs to be a bridge that converts Java objects produced by Hadoop 
```InputFormats``` to something that can be handled by Pyrolite\'s pickler (which will serialize the data into 
Python objects usable by PySpark), and vice versa. For this purpose, a ```Converter``` trait is introduced, 
along with a pair of default implementations that convert data to and from
Hadoop [Writables](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/Writable.html) (i.e., 
```WritableToJavaConverter``` and ```JavaToWritableConverter```).

Those default converters can only handle common Writable types. For custom Writables or non-Writable 
serialization frameworks, users need to supply custom converters. For example, ```ArrayWritable``` cannot be 
used to serialize arrays directly since it does not have a default no-arg constructor for deserializing. 
Users are expected to supply custom subclasses to handle arrays, which leads to custom converters.

Some additional converters for reading from (and writing to) HBase and Cassandra are included in the examples 
section of Spark distro, together with PySpark scripts that use them. Here, we will further illustrate the 
steps in writing custom PySpark converters, using [Apache Avro](http://avro.apache.org/docs/current/) serialization 
as an example.

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
PySpark will see. For example, for Avro ```ARRAY``` type, the Reflect mapping may produce primitive arrays,
Object arrays or ```java.util.Collection``` depending on its input. We want all of them to appear as 
Python ```List```, so we convert them all to ```java.util.Collection```,
which will then be pickled into Python ```List``` by [Pyrolite](https://github.com/irmen/Pyrolite).

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
```unpack*``` methods and the central switch ```fromAvro```, which handles the dispatching for all 14 
Avro schema types.


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

The complete source code for ```AvroWrapperToJavaConverter``` is in ```AvroConverters.scala```, while example
PySpark script for using the converter to read Avro files, in conjunction with ```AvroKeyInputFormat```, is given 
in ```avro_inputformat.py```.

One limitation of current ```Converter``` interface is the lack of means to set custom configuration options for 
converters. A future improvement could be, when instantiating converters, passing in the 
Hadoop [Configuration](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/conf/Configuration.html) object 
used by associated MapReduce jobs. This Configuration object serves as the vehicle to pass in any custom configuration
options a converter might need.
