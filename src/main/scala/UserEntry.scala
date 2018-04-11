import scala.util.Try

case class UserEntry(
  userId: String,
  productId: String,
  rating: Float,
  timestamp: Long
) {
  def ratingKey = s"$userId|$productId"
}

object UserEntry {
  def fromLine(line: String): Either[Throwable, UserEntry] = {
    line.split(",") match {
      case Array(userId, productId, rating, timestamp) =>
        for {
          rating <- Try(rating.toFloat).toEither
          timestamp <- Try(timestamp.toLong).toEither
        } yield {
          UserEntry(userId, productId, rating, timestamp)
        }
      case _ => Left(new Throwable("Parsing: Split error"))
    }
  }

  val day = 1 * 24 * 60 * 60 * 1000
  val penaltyCoef = 0.95

  def ratingWithPenalty(rating: Float, maxTimestamp: Long, timestamp: Long): Float = {
    val days = (maxTimestamp - timestamp) / day
    if (days == 0) {
      rating
    } else {
      (rating * (days * penaltyCoef)).toFloat
    }
  }
}
