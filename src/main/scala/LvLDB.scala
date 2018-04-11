import java.io.File

import akka.stream.scaladsl.Source
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options}
import collection.JavaConverters._

class LvLDB(db: DB) {
  def put(prefix: String, key: String, value: String) = {
    db.put(s"$prefix|$key".getBytes, value.getBytes())
  }

  def get(prefix: String, key: String): Option[String] = {
    Option(db.get(s"$prefix|$key".getBytes))
      .map(bytes => new String(bytes, "UTF-8"))
  }

  def keysValues = Source.fromIterator(() => db.iterator().asScala)

  def close() = db.close()
}

object LvLDB {
  def apply(path: String): LvLDB = {
    val levelDBStore: DB = Iq80DBFactory.factory.open(new File(path), new Options())
    new LvLDB(levelDBStore)
  }
}
