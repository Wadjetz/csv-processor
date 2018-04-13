import java.nio.file.Paths

import akka.stream.scaladsl.{FileIO, Framing}
import akka.util.ByteString

object Csv {
  def getLinesStream(path: String, lineEnd: String) = FileIO
    .fromPath(Paths.get(path))
    .via(Framing.delimiter(ByteString(lineEnd), 256, true)
      .map(_.utf8String))

  def parse[T](path: String, lineEnd: String, parsor: String => Either[Throwable, T]) = {
   getLinesStream(path, lineEnd)
      .map(parsor)
      .filter(_.isRight)
      .map(_.right.get)
  }
}
