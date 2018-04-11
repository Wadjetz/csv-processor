import java.io._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

object Main extends App {
  implicit val system = ActorSystem("QuickStart")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val db = LvLDB("levelDBStore")

  def userEntries = Csv.parse("./xag.csv", UserEntry.fromLine)

  userEntries
    .runWith(ProductProcessor.indexesSink)
    .onComplete(indexingResult => {
      indexingResult.map {
        case (_, _, maxTimestamp) => {
          userEntries
            .mapAsync(4)(ProductProcessor.processRating(maxTimestamp))
            .runWith(Sink.ignore)
            .onComplete(_ => {
              val aggregatePrinter: PrintWriter = new PrintWriter(new FileOutputStream(new File("aggratings.csv")))
              val userPrinter = new PrintWriter(new FileOutputStream(new File("lookupuser.csv")))
              val productPrinter = new PrintWriter(new FileOutputStream(new File("lookup_product.csv")))
              ProductProcessor.processKeys(aggregatePrinter, userPrinter, productPrinter).runWith(Sink.ignore)
                .onComplete(_ => {
                  aggregatePrinter.flush()
                  aggregatePrinter.close()
                  userPrinter.flush()
                  userPrinter.close()
                  productPrinter.flush()
                  productPrinter.close()
                  db.close()
                  system.terminate()
                })
            })
        }
      }
    })
}
