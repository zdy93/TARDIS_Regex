package bloomfilter.util

/**
  * copy from github.com/alexandrnikitin/bloom-filter-scala/
  * author: Alexandr Nikitin
  * reason: the jar file is compiled in java8 but my running environment is java7
  */
import sun.misc.{Unsafe => JUnsafe}

import scala.language.postfixOps
import scala.util.Try

object Unsafe {
  val unsafe: JUnsafe = Try {
    classOf[JUnsafe]
      .getDeclaredFields
      .find { field =>
        field.getType == classOf[JUnsafe]
      }
      .map { field =>
        field.setAccessible(true)
        field.get(null).asInstanceOf[JUnsafe]
      }
      .getOrElse(throw new IllegalStateException("Can't find instance of sun.misc.Unsafe"))
  } recover {
    case th: Throwable => throw new ExceptionInInitializerError(th)
  } get

}