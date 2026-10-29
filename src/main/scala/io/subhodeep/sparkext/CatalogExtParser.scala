package io.subhodeep.sparkext

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Tries the catalog-management grammar first and hands everything else to `delegate` unchanged.
 * Method list checked against Spark 3.5.8 ParserInterface + DataTypeParserInterface (javap).
 */
class CatalogExtParser(session: SparkSession, delegate: ParserInterface) extends ParserInterface {
  private val bootstrap = new Bootstrap(session)

  override def parsePlan(sqlText: String): LogicalPlan = {
    bootstrap.ensureLoaded()
    CatalogSqlParser.parse(sqlText).getOrElse(delegate.parsePlan(sqlText))
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)
  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)
  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)
  override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)
  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)
  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)
}

/**
 * Hand-written grammar:
 * {{{
 *   CREATE CATALOG [IF NOT EXISTS] name USING ('class' | GRAVITINO) [OPTIONS ( key [=] 'value' [, ...] )] [;]
 *   DROP CATALOG [IF EXISTS] name [;]
 *   SHOW CATALOGS [;]
 * }}}
 * A statement starting with CREATE CATALOG or DROP CATALOG is ours, so a syntax error in it is
 * reported here. SHOW CATALOGS followed by anything else (e.g. LIKE) goes to Spark's native command.
 */
private[sparkext] object CatalogSqlParser {
  sealed trait Tok { def pos: Int }
  final case class Word(text: String, pos: Int) extends Tok
  final case class Str(text: String, pos: Int) extends Tok
  final case class Sym(c: Char, pos: Int) extends Tok

  private def isWordChar(c: Char): Boolean =
    c.isLetterOrDigit || c == '_' || c == '.' || c == '-'

  /** Tokenizes as far as possible; returns the tokens read plus an error if it stopped early. */
  def tokenize(s: String): (Vector[Tok], Option[String]) = {
    val out = Vector.newBuilder[Tok]
    var i = 0
    val n = s.length
    while (i < n) {
      val c = s.charAt(i)
      if (c.isWhitespace) {
        i += 1
      } else if (s.startsWith("--", i)) {
        while (i < n && s.charAt(i) != '\n') i += 1
      } else if (s.startsWith("/*", i)) {
        val end = s.indexOf("*/", i + 2)
        if (end < 0) return (out.result(), Some(s"unterminated comment at position $i"))
        i = end + 2
      } else if (c == '\'') {
        val start = i
        val sb = new StringBuilder
        i += 1
        var closed = false
        while (i < n && !closed) {
          if (s.charAt(i) == '\'') {
            if (i + 1 < n && s.charAt(i + 1) == '\'') { sb.append('\''); i += 2 }
            else { closed = true; i += 1 }
          } else { sb.append(s.charAt(i)); i += 1 }
        }
        // Never echo literal contents: they may be secrets.
        if (!closed) return (out.result(), Some(s"unterminated string literal at position $start"))
        out += Str(sb.result(), start)
      } else if ("(),=;".indexOf(c) >= 0) {
        out += Sym(c, i)
        i += 1
      } else if (isWordChar(c)) {
        val start = i
        while (i < n && isWordChar(s.charAt(i))) i += 1
        out += Word(s.substring(start, i), start)
      } else {
        return (out.result(), Some(s"unexpected character '$c' at position $i"))
      }
    }
    (out.result(), None)
  }

  private def kw(t: Tok, k: String): Boolean = t match {
    case Word(w, _) => w.equalsIgnoreCase(k)
    case _ => false
  }

  def parse(sql: String): Option[LogicalPlan] = {
    val (toks, err) = tokenize(sql)
    if (toks.length < 2) return None
    val (a, b) = (toks(0), toks(1))
    if (kw(a, "CREATE") && kw(b, "CATALOG")) Some(owned("CREATE CATALOG", toks, err)(_.create()))
    else if (kw(a, "DROP") && kw(b, "CATALOG")) Some(owned("DROP CATALOG", toks, err)(_.drop()))
    else if (kw(a, "SHOW") && kw(b, "CATALOGS") && err.isEmpty &&
      toks.drop(2).forall { case Sym(';', _) => true; case _ => false }) Some(ShowCatalogsCmd())
    else None
  }

  private def owned(stmt: String, toks: Vector[Tok], err: Option[String])(
      f: Cursor => LogicalPlan): LogicalPlan = {
    err.foreach(e => throw new CatalogExtException(s"Syntax error in $stmt: $e."))
    f(new Cursor(stmt, toks, 2))
  }

  private final class Cursor(stmt: String, toks: Vector[Tok], private var i: Int) {
    private def peek: Option[Tok] = toks.lift(i)
    private def peekAt(o: Int): Option[Tok] = toks.lift(i + o)

    private def describe(t: Option[Tok]): String = t match {
      case Some(Word(w, p)) => s"'$w' at position $p"
      case Some(Str(_, p)) => s"a string literal at position $p"
      case Some(Sym(c, p)) => s"'$c' at position $p"
      case None => "end of input"
    }

    private def fail(expected: String): Nothing =
      throw new CatalogExtException(
        s"Syntax error in $stmt: expected $expected but found ${describe(peek)}.")

    private def acceptKw(k: String): Boolean =
      if (peek.exists(kw(_, k))) { i += 1; true } else false

    private def expectKw(k: String): Unit = if (!acceptKw(k)) fail(k.toUpperCase(Locale.ROOT))

    private def acceptSym(c: Char): Boolean = peek match {
      case Some(Sym(`c`, _)) => i += 1; true
      case _ => false
    }

    private def expectSym(c: Char): Unit = if (!acceptSym(c)) fail(s"'$c'")

    private def name(): String = peek match {
      case Some(Word(w, _)) => i += 1; w
      case _ => fail("a catalog name")
    }

    private def string(what: String): String = peek match {
      case Some(Str(s, _)) => i += 1; s
      case _ => fail(s"$what as a single-quoted string")
    }

    private def end(): Unit = {
      while (acceptSym(';')) {}
      if (peek.isDefined) fail("end of statement")
    }

    // `IF` is a keyword only when followed by NOT / EXISTS, so `if` still works as a name.
    def create(): LogicalPlan = {
      val ifNotExists = peek.exists(kw(_, "IF")) && peekAt(1).exists(kw(_, "NOT")) && {
        i += 1; expectKw("NOT"); expectKw("EXISTS"); true
      }
      val n = name()
      expectKw("USING")
      val impl = peek match {
        case Some(Word(w, _)) if w.equalsIgnoreCase(GravitinoSupport.Keyword) => i += 1; GravitinoSupport.Keyword
        case _ => string("the catalog class (or GRAVITINO)")
      }
      val options = if (acceptKw("OPTIONS")) optionList() else Nil
      end()
      CreateCatalogCmd(n, impl, options, ifNotExists)
    }

    def drop(): LogicalPlan = {
      val ifExists = peek.exists(kw(_, "IF")) && peekAt(1).exists(kw(_, "EXISTS")) && {
        i += 2; true
      }
      val n = name()
      end()
      DropCatalogCmd(n, ifExists)
    }

    private def optionList(): Seq[(String, String)] = {
      expectSym('(')
      if (acceptSym(')')) return Nil
      val opts = Seq.newBuilder[(String, String)]
      var more = true
      while (more) {
        val key = peek match {
          case Some(Word(w, _)) => i += 1; w
          case Some(Str(s, _)) => i += 1; s
          case _ => fail("an option key")
        }
        acceptSym('=')
        opts += key -> string(s"a value for option '$key'")
        if (!acceptSym(',')) { expectSym(')'); more = false }
      }
      opts.result()
    }
  }
}
