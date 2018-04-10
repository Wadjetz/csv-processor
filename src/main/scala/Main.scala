import java.io._
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{DB, Options}

import scala.concurrent.Future
import scala.util.Try

case class UserEntry(userId: String, productId: String, rating: Float, timestemp: Long) {
  def key = s"$userId|$productId"
}

object UserEntry {
  def fromLine(line: String): Either[Throwable, UserEntry] = {
    line.split(",") match {
      case Array(userId, productId, rating, timestemp) => {
        for {
          rating <- Try(rating.toFloat).toEither
          timestemp <- Try(timestemp.toLong).toEither
        } yield {
          UserEntry(userId, productId, rating, timestemp)
        }
      }
      case _ => Left(new Throwable("Parsing: Split error"))
    }
  }
}

class LvLDB(db: DB) {
  def put(prefix: String, key: String, value: String) = db.put(s"$prefix|$key".getBytes, value.getBytes())
  def get(prefix: String, key: String): Option[String] = Option(db.get(s"$prefix|$key".getBytes)).map(bytes => new String(bytes, "UTF-8") )
}

object Main extends App {
  implicit val system = ActorSystem("QuickStart")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  val levelDBStore: DB = Iq80DBFactory.factory.open(new File("levelDBStore"), new Options())

  val db = new LvLDB(levelDBStore)

  val day = 1 * 24 * 60 * 60 * 1000
  val penaltyCoef = 0.95

  def ratingWithPenalty(rating: Float, maxTimestimp: Long, timestimp: Long): Float = {
    val daysEcart = (maxTimestimp - timestimp) / day
    if (daysEcart == 0) {
      rating
    } else {
      (rating * (daysEcart * penaltyCoef)).toFloat
    }
  }

  def processRating(maxTimestimp: Long)(entry: UserEntry) = Future.successful {
    db.get("aggregate", entry.key).map(_.toFloat) match {
      case Some(previusRating) =>
        val newRating = previusRating + ratingWithPenalty(entry.rating, maxTimestimp, entry.timestemp)
        db.put("aggregate", entry.key, newRating.toString)
      case None =>
        db.put("aggregate", entry.key, ratingWithPenalty(entry.rating, maxTimestimp, entry.timestemp).toString)
    }
  }

  val indexesSink = Sink.fold[(Long, Long, Long), UserEntry]((0l, 0l, 0l)) {
    case ((userCount, productCount, timestemp), entry) => {
      val newUserCount = db.get("user", entry.userId) match {
        case Some(_) => userCount
        case None =>
          db.put("user", entry.userId, userCount.toString)
          userCount + 1
      }
      val newProductCount = db.get("product", entry.productId) match {
        case Some(_) => productCount
        case None =>
          db.put("product", entry.productId, productCount.toString)
          productCount + 1
      }
      val maxTimestimp = if (entry.timestemp > timestemp) {
        entry.timestemp
      } else {
        timestemp
      }
      (newUserCount, newProductCount, maxTimestimp)
    }
  }

  def csv = FileIO
                .fromPath(Paths.get("./xag.csv"))
                .via(Framing.delimiter(ByteString("\r\n"), 256, true)
                .map(_.utf8String))

  def userEntries = csv
    .map(UserEntry.fromLine)
    .filter(_.isRight)
    .map(_.right.get)

  userEntries
    .runWith(indexesSink)
    .onComplete(result => {
      println(s"result = $result")

      result.map {
        case (_, _, maxTimestimp) => {
          userEntries
            .mapAsync(4)(processRating(maxTimestimp))
            .runWith(Sink.ignore)
            .onComplete(res => {
              val agregatePrinter = new PrintWriter(new FileOutputStream(new File("aggratings.csv")))
              val userPrinter = new PrintWriter(new FileOutputStream(new File("lookupuser.csv")))
              val productPrinter = new PrintWriter(new FileOutputStream(new File("lookup_product.csv")))

              import collection.JavaConverters._

              Source.fromIterator(() => levelDBStore.iterator().asScala)
                .mapAsync(4) { entry =>
                  Future {
                    val key = new String(entry.getKey, "UTF-8")
                    val value = new String(entry.getValue, "UTF-8")
                    key.split("""\|""").toList match {
                      case List("aggregate", userId, productId) =>
                        for {
                          userIndex <- db.get("user", userId)
                          productIndex <- db.get("product", productId)
                        } yield {
                          agregatePrinter.println(s"$userIndex,$productIndex,$value")
                        }
                      case List("user", userId) =>
                        userPrinter.println(s"$userId,$value")
                      case List("product", productId) =>
                        productPrinter.println(s"$productId,$value")
                      case _ =>
                    }
                  }
                }.runWith(Sink.ignore)
                  .onComplete(_ => {
                    agregatePrinter.flush()
                    agregatePrinter.close()
                    userPrinter.flush()
                    userPrinter.close()
                    productPrinter.flush()
                    productPrinter.close()
                    levelDBStore.close()
                    system.terminate()
                  })
            })
        }
      }
    })
}
