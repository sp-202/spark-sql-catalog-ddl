package io.subhodeep.sparkext

import java.util.Locale

import scala.util.matching.Regex

/** Masks option values whose keys look sensitive, for logs, plan strings and error messages. */
private[sparkext] object Redact {
  val SensitiveKeyParts: Seq[String] = Seq("secret", "token", "password", "credential", "key")
  val Mask = "*********(redacted)"

  def isSensitive(key: String): Boolean = {
    val k = key.toLowerCase(Locale.ROOT)
    SensitiveKeyParts.exists(k.contains)
  }

  def options(opts: Seq[(String, String)]): Seq[(String, String)] =
    opts.map { case (k, v) => k -> (if (isSensitive(k)) Mask else v) }

  def render(opts: Seq[(String, String)]): String =
    options(opts).map { case (k, v) => s"$k=$v" }.mkString("{", ", ", "}")
}

/**
 * Resolves `${env:VAR_NAME}` references inside option values at command execution time.
 * Resolved values are returned to the caller only; they are never logged or put in messages.
 */
private[sparkext] object EnvSubstitution {
  private val Ref: Regex = """\$\{env:([^}]*)\}""".r
  private val VarName: Regex = "^[A-Za-z_][A-Za-z0-9_]*$".r

  def resolve(key: String, value: String, env: String => Option[String] = sys.env.get): String =
    Ref.replaceAllIn(value, m => {
      val name = m.group(1)
      if (VarName.findFirstIn(name).isEmpty) {
        throw new CatalogExtException(
          s"Option '$key' references an invalid environment variable name '$name'.")
      }
      val resolved = env(name).getOrElse(throw new CatalogExtException(
        s"Environment variable '$name' referenced by option '$key' is not set."))
      Regex.quoteReplacement(resolved)
    })
}
