package com

import java.time.Duration as JavaDuration
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.NANOSECONDS

package object rides {

  implicit class JavaDurationOps(val duration: JavaDuration) extends AnyVal {
    def asScala: FiniteDuration = FiniteDuration(duration.toNanos, NANOSECONDS)
  }

  // trait StatusTag

}
