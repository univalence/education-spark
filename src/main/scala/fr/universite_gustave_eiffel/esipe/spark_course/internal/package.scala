package fr.universite_gustave_eiffel.esipe.spark_course

import java.io.File
import scala.util.{Failure, Success, Try}

package object internal {

  private final case class ActivateContext(label: String)
  private var activatedContexts: Seq[ActivateContext] = Seq.empty

  def exercise(label: String, activated: Boolean)(f: => Unit): Unit =
    if (activated) {
      activatedContexts = activatedContexts :+ ActivateContext(label)
      part(activatedContexts.map(_.label).mkString(" > "))

      try { f } catch {
        case e: Exception =>
          throw new PartException(label, e)
      } finally {
        activatedContexts = activatedContexts.init
      }
    } else ()

  def exercise(label: String)(f: => Unit): Unit = exercise(label, activated = true)(f)

  class PartException(label: String, cause: Throwable)
      extends RuntimeException(s"""Exception caught in part "$label"""", cause)

  def part(label: String): Unit = println(s"${Console.YELLOW}+++ PART $label${Console.RESET}")

  def check(label: String, condition: => Boolean): Unit = {
    val result = Try(condition)

    val display =
      result match {
        case Success(true)  => s"${Console.GREEN}OK${Console.RESET}"
        case Success(false) => s"${Console.YELLOW}FAILED${Console.RESET}"
        case Failure(e)     => s"${Console.RED}ERROR: ${e.getClass.getCanonicalName}: ${e.getMessage}${Console.RESET}"
      }

    println(("\t" * activatedContexts.size) + s">>> $label: $display")
  }

  def clean(outputFilename: String): Unit = {
    val file = new File(outputFilename)

    if (file.exists()) {
      if (file.isDirectory) {
        file.listFiles().toList.foreach(_.delete())
        file.delete()
      } else {
        file.delete()
      }
    }
  }

}
