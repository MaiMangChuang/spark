package traffic.utilscala
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

object TestUDAF extends  UserDefinedAggregateFunction{
  /**
    * 设置输入数据的数据类型
    * 例如:override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
    * 这里设置了输入数据为一个，名称为inputColumn，且其数据类型为LongType
    * @return
    */
  override def inputSchema: StructType = ???

  /**
    * 设置缓冲区内保留的数据类型 （可以理解为方法的成员变量，计算使用）
    * 例如:def bufferSchema: StructType = {StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)}
    * 这里设置了缓冲区使用两个数据，一个名称为sum，类型为LongType，另一个名称为count，类型为LongType
    * @return
    */
  override def bufferSchema: StructType = ???

  /**
    * 设置最终返回的数据类型
    * @return
    */
  override def dataType: DataType = ???

  /**
    * 设置该函数是否为幂等函数
    * 幂等函数:即只要输入的数据相同，结果一定相同
    * true表示是幂等函数，false表示不是
    * 例如：
    * override def deterministic: Boolean = true
    * @return
    */
  override def deterministic: Boolean = ???

  /**
    * 初始化缓冲区的数据，即给bufferSchema中设置的数据进行初始化
    * 例如：
    * override def initialize(buffer: MutableAggregationBuffer): Unit = {
    * buffer(0) = 0L
    * buffer(1) = 0L }
    * buffer(0)即为bufferSchema设置的第一个数据，buffer(1)是第二个
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  /**
    * 使用来自“input”的新输入数据更新给定的聚合缓冲区“buffer”
    * 每个输入行调用一次。
    * 设置当使用该聚合函数时，一条记录与另一条记录之间的聚合时，缓冲区内数据该如何计算
    * 例如：
    * override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    * if (!input.isNullAt(0)) {
    *  buffer(0) = buffer.getLong(0) + input.getLong(0)
    *  buffer(1) = buffer.getLong(1) + 1 }}
    * @param buffer 表示原本缓冲区内的数据
    * @param input 新的输入数据
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  /**
    * 合并两个聚合缓冲区并将更新后的缓冲区值存储回“buffer1”。
    * 当我们将两个部分聚合的数据合并在一起时，就会调用这个函数。
    * 例如：
    * override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    * buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    * buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1) }
    *
    * @param buffer1 缓冲区数据1
    * @param buffer2 缓冲区数据2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  /**
    * 计算最终结果（最后一步计算，利用已经计算完成的缓冲区内的值，得到我们要的最终结果）
    * 例如：
    * override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
    * @param buffer 缓冲区数据
    * @return 最终计算结果
    */
  override def evaluate(buffer: Row): Any = ???
}

object a extends UserDefinedAggregateFunction {
  // Data types of input arguments of this aggregate function
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
  // Data types of values in the aggregation buffer
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }
  // The data type of the returned value
  def dataType: DataType = DoubleType
  // Whether this function always returns the same output on the identical input
  def deterministic: Boolean = true
  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L

  }
  // Updates the given aggregation buffer `buffer` with new input data from `input`
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }
  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  // Calculates the final result
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}

