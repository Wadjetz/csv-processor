import java.io.File

case class Cli (
  input: File = new File("./input.csv"),
  aggratings: File = new File("./aggratings.csv"),
  lookupuser: File = new File("./lookupuser.csv"),
  lookupProduct: File = new File("./lookup_product.csv"),
  lineEnd: String = "\r\n"
)

object Cli {
  val parser = new scopt.OptionParser[Cli]("csv_processor") {
    head("csv_processor", "0.0.1")

    opt[File]('i', "input").required().valueName("<file>").
      action((x, c) => c.copy(input = x)).
      text("input is a required file property")

    opt[File]('a', "aggratings").valueName("<file>").
      action((x, c) => c.copy(aggratings = x)).
      text("aggratings is a file property")

    opt[File]('u', "lookupuser").valueName("<file>").
      action((x, c) => c.copy(lookupuser = x)).
      text("lookupuser is a file property")

    opt[File]('p', "lookupProduct").valueName("<file>").
      action((x, c) => c.copy(lookupProduct = x)).
      text("lookupProduct is a file property")

    opt[String]('e', "lineEnd").valueName("line end").
      action((x, c) => c.copy(lineEnd = x)).
      text("""line end "\r\n" or "\n"""")
  }
}

