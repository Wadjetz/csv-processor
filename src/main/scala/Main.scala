import java.io._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.ExecutionContext

object Main extends App {
  implicit val system: ActorSystem = ActorSystem("QuickStart")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val db: LvLDB = LvLDB("levelDBStore")

  def userEntries = Csv.parse("./xag.csv", UserEntry.fromLine)

  userEntries
    .runWith(ProductsProcessor.indexesSink)
    .onComplete(indexingResult => {
      indexingResult.map {
        case (_, _, maxTimestamp) => {
          userEntries
            .mapAsync(4)(ProductsProcessor.processRating(maxTimestamp))
            .runWith(Sink.ignore)
            .onComplete(_ => {
              val aggregatePrinter: PrintWriter = new PrintWriter(new FileOutputStream(new File("aggratings.csv")))
              val userPrinter = new PrintWriter(new FileOutputStream(new File("lookupuser.csv")))
              val productPrinter = new PrintWriter(new FileOutputStream(new File("lookup_product.csv")))
              ProductsProcessor.processKeys(aggregatePrinter, userPrinter, productPrinter).runWith(Sink.ignore)
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
