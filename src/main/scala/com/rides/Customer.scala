/*
package com.rides

import akka.actor.FSM.StateTimeout
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.persistence.journal.EventAdapter
import akka.persistence.typed.{EventSeq, PersistenceId}
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.Effect
import Customer.DomainEvent

import java.time.Instant
import scala.concurrent.duration.DurationInt

//https://doc.akka.io/docs/akka/current/typed/persistence-fsm.html#eventsourced-behaviors-as-finite-state-machines
object Customer {

  /** EventSourced behaviors as finite state machines
 *
 * A customer can be in the following states:
 *
 * Looking around Shopping (has something in their basket) Inactive Paid
 */

  final case class ShoppingCart(item: List[Item] = List.empty) { self =>
    def addItem(item: Item): ShoppingCart =
      self.copy(item = item :: self.item)
  }

  final case class ShoppingCart2(items: Map[String, Item], checkedOutTime: Option[Instant] = None) {
    def isOpen: Boolean     = checkedOutTime.isEmpty
    def checkedOut: Boolean = !isOpen
  }

  final case class Item(orderId: String, itemName: String, itemPrice: BigDecimal, itemQuantity: Int)

  sealed trait StateTag
  object StateTag {
    final case object LookingAround extends StateTag
    final case object Shopping      extends StateTag
    final case object Inactive      extends StateTag
    final case object Paid          extends StateTag
  }
  final case class CustomerState(cart: ShoppingCart, tag: StateTag)

  sealed trait Command
  object Command {
    final case class AddItem(item: Item)                             extends Command
    final case object Buy                                            extends Command
    final case object Leave                                          extends Command
    final case class GetCurrentCart(replyTo: ActorRef[ShoppingCart]) extends Command
    final case object Timeout                                        extends Command
  }

  sealed trait DomainEvent
  object DomainEvent {
    case class ItemAdded(item: Item) extends DomainEvent
    object OrderDiscarded            extends DomainEvent
    object OrderExecuted             extends DomainEvent
    object CustomerInactive          extends DomainEvent
  }

  val timerTimeout = 5.second

  def commandHandler(
    state: CustomerState,
    cmd: Command
  )(implicit timers: TimerScheduler[Command]): Effect[DomainEvent, CustomerState] =
    state.tag match {
      case StateTag.LookingAround =>
        cmd match {
          case Command.AddItem(item) =>
            Effect
              .persist(DomainEvent.ItemAdded(item))
              .thenRun(_ => timers.startSingleTimer(StateTimeout, Command.Timeout, timerTimeout))

          case Command.GetCurrentCart(replyTo) =>
            replyTo ! state.cart
            Effect.none

          case _ =>
            Effect.none
        }

      case StateTag.Shopping =>
        cmd match {
          case Command.AddItem(item) =>
            Effect
              .persist(DomainEvent.ItemAdded(item))
              .thenRun(_ => timers.startSingleTimer(StateTimeout, Command.Timeout, timerTimeout))

          case Command.Buy =>
            Effect
              .persist(DomainEvent.OrderExecuted)
              .thenRun(_ => timers.cancel(StateTimeout))

          case Command.Leave =>
            Effect
              .persist(DomainEvent.OrderDiscarded)
              .thenStop()

          case Command.GetCurrentCart(replyTo) =>
            replyTo ! state.cart
            Effect.none

          case Command.Timeout =>
            Effect.persist(DomainEvent.CustomerInactive)

        }

      case StateTag.Inactive =>
        cmd match {
          case Command.AddItem(item) =>
            Effect
              .persist(DomainEvent.ItemAdded(item))
              .thenRun(_ => timers.startSingleTimer(StateTimeout, Command.Timeout, timerTimeout))

          case Command.Timeout =>
            Effect.persist(DomainEvent.OrderDiscarded)

          case _ =>
            Effect.none
        }

      case StateTag.Paid =>
        cmd match {
          case Command.Leave =>
            Effect.stop()

          case Command.GetCurrentCart(replyTo) =>
            replyTo ! state.cart
            Effect.none

          case _ =>
            Effect.none
        }
    }

  def eventHandler(state: CustomerState, event: DomainEvent): CustomerState =
    state.tag match {
      case StateTag.LookingAround =>
        event match {
          case DomainEvent.ItemAdded(item) => CustomerState(state.cart.addItem(item), StateTag.Shopping)
          case _                           => state
        }

      case StateTag.Shopping =>
        event match {
          case DomainEvent.ItemAdded(item)  => CustomerState(state.cart.addItem(item), StateTag.Shopping)
          case DomainEvent.OrderExecuted    => CustomerState(state.cart, StateTag.Paid)
          case DomainEvent.OrderDiscarded   => state // will be stopped
          case DomainEvent.CustomerInactive => CustomerState(state.cart, StateTag.Inactive)
        }

      case StateTag.Inactive =>
        event match {
          case DomainEvent.ItemAdded(item) => CustomerState(state.cart.addItem(item), StateTag.Shopping)
          case DomainEvent.OrderDiscarded  => state // will be stopped
          case _                           => state
        }

      case StateTag.Paid =>
        state // no events after paid
    }

  def apply(customerId: String): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { implicit timers: TimerScheduler[Command] =>
        EventSourcedBehavior[Command, DomainEvent, CustomerState](
          persistenceId = PersistenceId.ofUniqueId(customerId),
          emptyState = CustomerState(ShoppingCart(), StateTag.LookingAround),
          commandHandler = { (state, cmd) => commandHandler(state, cmd) },
          eventHandler = { (state, event) => eventHandler(state, event) }
        )
          .snapshotAdapter(???)
          .eventAdapter(new JsonRenamedFieldAdapter2)
          .receiveSignal {
            case (state, akka.persistence.typed.RecoveryCompleted) =>
              ctx.log.debug("RecoveryCompleted {}", state)
            case (state, akka.persistence.typed.RecoveryFailed(ex)) =>
              ctx.log.error("RecoveryFailed: ", ex)
            case (state, akka.actor.typed.PostStop)   =>
            case (state, akka.actor.typed.PreRestart) =>
            // case (state, akka.actor.typed.Terminated()) =>
          }
      }
    }
}

class JsonRenamedFieldAdapter2 extends akka.persistence.typed.EventAdapter[DomainEvent, DomainEvent] {

  override def manifest(event: DomainEvent): String = event.getClass.getSimpleName

  override def toJournal(e: DomainEvent): DomainEvent = ???

  override def fromJournal(p: DomainEvent, manifest: String): EventSeq[DomainEvent] =
    EventSeq.single(p)
}
 */

//https://sharonsyra.medium.com/akka-persistence-journal-migration-77bba63b1641
/*

class JsonRenamedFieldAdapter extends EventAdapter {
  import spray.json._
  val marshaller = new ExampleJsonMarshaller

  val V1 = "v1"
  val V2 = "v2"

  // this could be done independently for each event type
  override def manifest(event: Any): String = V2

  override def toJournal(event: Any): JsObject =
    marshaller.toJson(event)

  override def fromJournal(event: Any, manifest: String): akka.persistence.journal.EventSeq = event match {
    case json: JsObject =>
      akka.persistence.journal.EventSeq(marshaller.fromJson(manifest match {
        case V1      => rename(json, "code", "seatNr")
        case V2      => json // pass-through
        case unknown => throw new IllegalArgumentException(s"Unknown manifest: $unknown")
      }))
    case _ =>
      val c = event.getClass
      throw new IllegalArgumentException("Can only work with JSON, was: %s".format(c))
  }

  /*def rename(json: JsObject, from: String, to: String): JsObject = {
    val value = json.fields(from)
    val withoutOld = json.fields - from
    JsObject(withoutOld + (to -> value))
  }*/
}
 */
