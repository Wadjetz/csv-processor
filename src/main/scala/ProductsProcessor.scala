import java.io.PrintWriter
import akka.stream.scaladsl.Sink
import scala.concurrent.Future

object ProductsProcessor {

  val aggregatePrefix = "aggregate"
  val userPrefix = "user"
  val productPrefix = "product"

  def processRating(maxTimestamp: Long)(entry: UserEntry)(implicit db: LvLDB) = Future.successful {
    db.get(aggregatePrefix, entry.ratingKey).map(_.toFloat) match {
      case Some(previousRating) =>
        val newRating = previousRating + UserEntry.ratingWithPenalty(entry.rating, maxTimestamp, entry.timestamp)
        db.put(aggregatePrefix, entry.ratingKey, newRating.toString)
      case None =>
        db.put(aggregatePrefix, entry.ratingKey, UserEntry.ratingWithPenalty(entry.rating, maxTimestamp, entry.timestamp).toString)
    }
  }

  def indexesSink(implicit db: LvLDB) = Sink.fold[(Long, Long, Long), UserEntry]((0l, 0l, 0l)) {
    case ((userCount, productCount, timestamp), entry) =>
      val newUserCount = db.get(userPrefix, entry.userId) match {
        case Some(_) => userCount
        case None =>
          db.put(userPrefix, entry.userId, userCount.toString)
          userCount + 1
      }
      val newProductCount = db.get(productPrefix, entry.productId) match {
        case Some(_) => productCount
        case None =>
          db.put(productPrefix, entry.productId, productCount.toString)
          productCount + 1
      }
      val maxTimestamp = if (entry.timestamp > timestamp) {
        entry.timestamp
      } else {
        timestamp
      }
      (newUserCount, newProductCount, maxTimestamp)
  }

  def writeKeys(key: String, value: String, aggregatePrinter: PrintWriter, userPrinter: PrintWriter, productPrinter: PrintWriter)(implicit db: LvLDB) = {
    key.split("""\|""").toList match {
      case List("aggregate", userId, productId) =>
        for {
          userIndex <- db.get(userPrefix, userId)
          productIndex <- db.get(productPrefix, productId)
        } yield {
          aggregatePrinter.println(s"$userIndex,$productIndex,$value")
        }
      case List("user", userId) =>
        userPrinter.println(s"$userId,$value")
      case List("product", productId) =>
        productPrinter.println(s"$productId,$value")
      case _ =>
    }
  }

  def processKeys(aggregatePrinter: PrintWriter, userPrinter: PrintWriter, productPrinter: PrintWriter)(implicit db: LvLDB) = {
    db.keysValues
      .mapAsync(4) { entry =>
        Future.successful {
          val key = new String(entry.getKey, "UTF-8")
          val value = new String(entry.getValue, "UTF-8")
          ProductsProcessor.writeKeys(key, value, aggregatePrinter, userPrinter, productPrinter)
        }
      }
  }
}
