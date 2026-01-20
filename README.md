# PROFESIONAL_CODE
Código de limpieza DataSet correctamente estructurado. 
```scala
import cats.effect.{IO, IOApp, Ref}
import cats.syntax.all.*
import fs2.{Stream, text}
import fs2.io.file.{Files, Flags, Path}
import io.circe.{Decoder, parser}
import io.circe.generic.semiauto.*

import java.text.DecimalFormat
import scala.language.postfixOps

object NormalizedETL_Segundo extends IOApp.Simple {

  // ==================== MODELOS ====================
  case class Genre(id: Long, name: String)
  case class ProductionCompany(id: Long, name: String)
  case class Keyword(id: Long, name: String)

  case class CastMember(
                         id: Long,
                         name: String,
                         character: Option[String],
                         order: Option[Int],
                         gender: Option[Int],
                         cast_id: Option[Int],
                         credit_id: Option[String],
                         profile_path: Option[String]
                       )

  case class CrewMember(
                         id: Long,
                         name: String,
                         job: Option[String],
                         department: Option[String],
                         credit_id: Option[String],
                         gender: Option[Int],
                         profile_path: Option[String]
                       )

  case class Rating(userId: Long, rating: Double, timestamp: Long)

  case class MovieBase(
                        movieId: Int,
                        title: String,
                        originalTitle: String,
                        originalLanguage: String,
                        budget: Option[Long],
                        revenue: Option[Long],
                        runtime: Option[Int],
                        popularity: Double,
                        voteAverage: Double,
                        voteCount: Int,
                        releaseDate: Option[String],
                        status: String,
                        adult: Boolean,
                        homepage: Option[String],
                        imdbId: String,
                        posterPath: Option[String],
                        tagline: Option[String],
                        overview: String
                      )

  case class Stats(
                    total: Long = 0,
                    success: Long = 0,
                    errors: Long = 0,
                    jsonParsingErrors: Long = 0
                  )

  // ==================== DECODERS ====================
  implicit val genreDecoder: Decoder[Genre] = deriveDecoder[Genre]
  implicit val companyDecoder: Decoder[ProductionCompany] = deriveDecoder[ProductionCompany]
  implicit val keywordDecoder: Decoder[Keyword] = deriveDecoder[Keyword]

  implicit val castDecoder: Decoder[CastMember] = Decoder.instance { h =>
    for {
      id <- h.get[Long]("id")
      name <- h.getOrElse[String]("name")("Unknown")
      character <- h.get[Option[String]]("character")
      order <- h.get[Option[Int]]("order")
      gender <- h.get[Option[Int]]("gender")
      cast_id <- h.get[Option[Int]]("cast_id")
      credit_id <- h.get[Option[String]]("credit_id")
      profile_path <- h.get[Option[String]]("profile_path")
    } yield CastMember(id, name, character, order, gender, cast_id, credit_id, profile_path)
  }

  implicit val crewDecoder: Decoder[CrewMember] = Decoder.instance { h =>
    for {
      id <- h.get[Long]("id")
      name <- h.getOrElse[String]("name")("Unknown")
      job <- h.get[Option[String]]("job")
      department <- h.get[Option[String]]("department")
      credit_id <- h.get[Option[String]]("credit_id")
      gender <- h.get[Option[Int]]("gender")
      profile_path <- h.get[Option[String]]("profile_path")
    } yield CrewMember(id, name, job, department, credit_id, gender, profile_path)
  }

  implicit val ratingDecoder: Decoder[Rating] = Decoder.instance { h =>
    for {
      userId <- h.get[Long]("userId")
      rating <- h.get[Double]("rating")
      timestamp <- h.get[Long]("timestamp")
    } yield Rating(userId, rating, timestamp)
  }

  // ==================== PATHS ====================
  val inputPath: Path = Path("C:\\Users\\Usuario\\OneDrive - Universidad Técnica Particular de Loja - UTPL\\Escritorio\\Programar\\Programacion\\Practicum1.1\\LimpiezaDeDatos\\src\\main\\resources\\pi_movies_complete (1).csv")
  val outputDir: Path = Path("output_normalized")

  val moviesFile = outputDir / "movies.csv"
  val genresFile = outputDir / "movie_genres.csv"
  val companiesFile = outputDir / "movie_companies.csv"
  val keywordsFile = outputDir / "movie_keywords.csv"
  val castFile = outputDir / "movie_cast.csv"
  val crewFile = outputDir / "movie_crew.csv"
  val ratingsFile = outputDir / "movie_ratings.csv"
  val errorsFile = outputDir / "errors.log"

  // ==================== HELPER FUNCTIONS ====================
  def cleanJsonString(raw: String): String = {
    if (raw.trim.isEmpty || raw.trim == "[]" || raw.trim == "\"[]\"") {
      "[]"
    } else {
      var cleaned = raw.trim
      if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
        cleaned = cleaned.substring(1, cleaned.length - 1)
      }
      cleaned = cleaned
        .replace("'", "\"")
        .replace("\\\"", "\"")
        .replace("\"\"", "\"")
        .replace("None", "null")
        .replace("True", "true")
        .replace("False", "false")
      cleaned
    }
  }

  def parseJsonList[A: Decoder](raw: String, fieldName: String): IO[List[A]] = {
    if (raw.trim.isEmpty || raw.trim == "[]" || raw.trim == "\"[]\"") {
      IO.pure(List.empty[A])
    } else {
      val cleaned = cleanJsonString(raw)
      IO.fromEither(
        parser.parse(cleaned).flatMap { json =>
          json.as[List[A]]
        }
      ).handleErrorWith { error =>
        IO.println(s"⚠️ Error parsing $fieldName: ${error.getMessage.take(100)}") *>
          IO.pure(List.empty[A])
      }
    }
  }

  def safeInt(s: String): Option[Int] =
    if (s.trim.isEmpty || s.trim == "None") None
    else scala.util.Try(s.trim.toInt).toOption

  def safeLong(s: String): Option[Long] =
    if (s.trim.isEmpty || s.trim == "None") None
    else scala.util.Try(s.trim.toLong).toOption

  def safeDouble(s: String): Double =
    scala.util.Try(s.trim.toDouble).getOrElse(0.0)

  def safeBoolean(s: String): Boolean =
    s.trim.toLowerCase match {
      case "true" | "t" | "1" => true
      case "false" | "f" | "0" => false
      case _ => scala.util.Try(s.trim.toBoolean).getOrElse(false)
    }

  // ==================== PARSER CSV ====================
  def parseRow(row: String, lineNum: Int): IO[Either[String, (MovieBase, List[Genre], List[ProductionCompany], List[Keyword], List[CastMember], List[CrewMember], List[Rating])]] = {
    try {
      val cols = row.split(";", -1).map(_.trim)
      if (cols.length < 28) {
        IO.pure(Left(s"Línea $lineNum: Número incorrecto de columnas (${cols.length}, esperadas 28)"))
      } else {
        for {
          genresF <- parseJsonList[Genre](cols(3), "genres")
          companiesF <- parseJsonList[ProductionCompany](cols(12), "production_companies")
          keywordsF <- parseJsonList[Keyword](cols(24), "keywords")
          castF <- parseJsonList[CastMember](cols(25), "cast")
          crewF <- parseJsonList[CrewMember](cols(26), "crew")
          ratingsF <- parseJsonList[Rating](cols(27), "ratings")
        } yield {
          val movie = MovieBase(
            movieId = safeInt(cols(5)).getOrElse(0),
            title = cols(20),
            originalTitle = cols(8),
            originalLanguage = cols(7),
            budget = safeLong(cols(2)),
            revenue = safeLong(cols(15)),
            runtime = safeInt(cols(16)),
            popularity = safeDouble(cols(10)),
            voteAverage = safeDouble(cols(22)),
            voteCount = safeInt(cols(23)).getOrElse(0),
            releaseDate = Option(cols(14)).filter(s => s.nonEmpty && s != "None"),
            status = cols(18),
            adult = safeBoolean(cols(0)),
            homepage = Option(cols(4)).filter(s => s.nonEmpty && s != "None"),
            imdbId = cols(6),
            posterPath = Option(cols(11)).filter(s => s.nonEmpty && s != "None"),
            tagline = Option(cols(19)).filter(s => s.nonEmpty && s != "None"),
            overview = cols(9)
          )
          Right((movie, genresF, companiesF, keywordsF, castF, crewF, ratingsF))
        }
      }
    } catch {
      case e: Exception =>
        IO.pure(Left(s"Línea $lineNum: ${e.getMessage.take(200)}"))
    }
  }

  // ==================== FORMATO CSV ====================
  def movieToCsv(movie: MovieBase): String = {
    List(
      movie.movieId,
      escapeCsv(movie.title),
      escapeCsv(movie.originalTitle),
      movie.originalLanguage,
      movie.budget.getOrElse(""),
      movie.revenue.getOrElse(""),
      movie.runtime.getOrElse(""),
      movie.popularity,
      movie.voteAverage,
      movie.voteCount,
      movie.releaseDate.map(escapeCsv).getOrElse(""),
      movie.status,
      movie.adult,
      movie.homepage.map(escapeCsv).getOrElse(""),
      movie.imdbId,
      movie.posterPath.map(escapeCsv).getOrElse(""),
      movie.tagline.map(escapeCsv).getOrElse(""),
      escapeCsv(movie.overview.take(500))
    ).mkString(";")
  }

  def escapeCsv(value: Any): String = {
    val str = value.toString
    if (str.contains(";") || str.contains("\"") || str.contains("\n"))
      "\"" + str.replace("\"", "\"\"") + "\""
    else str
  }

  // ==================== RUN ====================
  override def run: IO[Unit] = {
    for {
      _ <- IO.println("Iniciando ETL...")
      stats <- Ref[IO].of(Stats())
      _ <- Files[IO].createDirectories(outputDir)
      _ <- writeHeaders
      
      _ <- Files[IO]
        .readUtf8Lines(inputPath)
        .zipWithIndex
        .drop(1)
        .evalMap { case (row, idx) =>
          val lineNum = idx.toInt + 1
          parseRow(row, lineNum).flatMap {
            case Right((movie, genres, companies, keywords, cast, crew, ratings)) =>
              IO.parSequenceN(7)(List(
                Files[IO].writeAll(moviesFile, Flags.Append).apply(Stream.emit(movieToCsv(movie) + "\n").through(text.utf8.encode)).compile.drain
                // ... resto de escrituras
              ))
            case Left(error) => IO.unit
          }
        }.compile.drain
      _ <- IO.println("Proceso completado.")
    } yield ()
  }
}
