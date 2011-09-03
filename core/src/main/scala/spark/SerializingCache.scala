package spark

import java.io._

/**
 * Wrapper around a BoundedMemoryCache that stores serialized objects as
 * byte arrays in order to reduce storage cost and GC overhead
 */
class SerializingCache extends Cache with Logging {
  val bmc = new BoundedMemoryCache

  override def put(key: Any, value: Any) {
    val ser = SparkEnv.get.serializer.newInstance()
    try {
      bmc.put(key, ser.serialize(value))
    } catch {
      case e: com.esotericsoftware.kryo.SerializationException =>
        logError("Couldn't serialize " + key.toString)
        throw e
    }
  }

  override def get(key: Any): Any = {
    val bytes = bmc.get(key)
    if (bytes != null) {
      val ser = SparkEnv.get.serializer.newInstance()
      return ser.deserialize(bytes.asInstanceOf[Array[Byte]])
    } else {
      return null
    }
  }
}
