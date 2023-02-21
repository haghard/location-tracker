package akka.cluster.ddata

import akka.stream.Attributes
import akka.stream.FanOutShape2
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler

final case class TwoWayRouter[T, A, B](validation: T => A Either B) extends GraphStage[FanOutShape2[T, A, B]] {

  val in   = Inlet[T]("in")
  val out0 = Outlet[A]("out0")
  val out1 = Outlet[B]("out1")

  override def shape = new FanOutShape2[T, A, B](in, out0, out1)

  override def createLogic(inheritedAttributes: Attributes) =
    new GraphStageLogic(shape) {
      var initialized                                           = false
      var pending: Option[(A, Outlet[A]) Either (B, Outlet[B])] = None

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            pending = validation(grab(in)) match {
              case Right(v) => Some(Right(v, out1))
              case Left(v)  => Some(Left(v, out0))
            }
            tryPush
          }
        }
      )

      List(out0, out1).foreach {
        setHandler(
          _,
          new OutHandler {
            override def onPull() = {
              if (!initialized) {
                initialized = true
                tryPull(in)
              }
              tryPush
            }
          }
        )
      }

      private def tryPushOut1(elem: B, out1: Outlet[B]): Unit =
        if (isAvailable(out1)) {
          push(out1, elem)
          tryPull(in)
          pending = None

          if (isClosed(in))
            completeStage()
        }

      private def tryPushOut0(elem: A, out0: Outlet[A]): Unit =
        if (isAvailable(out0)) {
          push(out0, elem)
          tryPull(in)
          pending = None

          if (isClosed(in))
            completeStage()
        }

      private def tryPush(): Unit =
        pending.foreach { value =>
          value match {
            case Right((v, out1)) => tryPushOut1(v, out1)
            case Left((v, out0))  => tryPushOut0(v, out0)
          }
        }
    }
}
