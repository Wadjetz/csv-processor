import java.io._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.ExecutionContext

object Main extends App {
  Cli.parser.parse(args, Cli()) match {
    case Some(config) =>
      implicit val system: ActorSystem = ActorSystem("QuickStart")
      implicit val ec: ExecutionContext = system.dispatcher
      implicit val materializer: Materializer = ActorMaterializer()
      val tmpPath = s"${System.getProperty("java.io.tmpdir")}levelDBStore-${Math.random()}"
      implicit val db: LvLDB = LvLDB(tmpPath)

      def shutdown() = {
        db.close()
        system.terminate()
      }

      def userEntries = Csv.parse(config.input.getAbsolutePath, config.lineEnd, UserEntry.fromLine)

      userEntries
        .runWith(ProductsProcessor.indexesSink)
        .map {
          case (_, _, maxTimestamp) =>
            userEntries
              .mapAsync(4)(ProductsProcessor.processRating(maxTimestamp))
              .runWith(Sink.ignore)
              .onComplete(_ => {
                val aggregatePrinter = new PrintWriter(new FileOutputStream(config.aggratings))
                val userPrinter = new PrintWriter(new FileOutputStream(config.lookupuser))
                val productPrinter = new PrintWriter(new FileOutputStream(config.lookupProduct))
                ProductsProcessor.processKeys(aggregatePrinter, userPrinter, productPrinter).runWith(Sink.ignore)
                  .onComplete(_ => {
                    aggregatePrinter.flush()
                    aggregatePrinter.close()
                    userPrinter.flush()
                    userPrinter.close()
                    productPrinter.flush()
                    productPrinter.close()
                    shutdown()
                  })
              })
        }
        .failed.map(error => {
          println(error.getMessage)
          shutdown()
        })

    case None =>
  }
}
