package akka.cluster

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.ReachabilityEvent
import akka.cluster.ClusterEvent.ReachableMember
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.typed.ClusterStateSubscription
import akka.cluster.typed.Subscribe

//https://medium.com/@bandopadhyaysamik/akka-cluster-membership-using-behavior-da4d540d68f8
object ClusterListener {

  sealed trait Event

  private final case class ReachabilityChange(reachabilityEvent: ReachabilityEvent) extends Event

  private final case class MemberChange(event: MemberEvent) extends Event

  def apply(): Behavior[Event] =
    Behaviors.setup { ctx =>
      ctx.log.info(s"..... Cluster Listener is up .....")
      val subscriptions: ActorRef[ClusterStateSubscription] = typed.Cluster(ctx.system).subscriptions

      val memberEventAdapter: ActorRef[MemberEvent] =
        ctx.messageAdapter[MemberEvent] { event: MemberEvent => MemberChange(event) }

      subscriptions.tell(Subscribe(memberEventAdapter, classOf[MemberEvent]))

      val reachabilityAdapter = ctx.messageAdapter(re => ReachabilityChange(re))
      subscriptions.tell(Subscribe(reachabilityAdapter, classOf[ReachabilityEvent]))

      Behaviors.receiveMessage { msg =>
        msg match {
          case ReachabilityChange(reachabilityEvent) =>
            reachabilityEvent match {
              case UnreachableMember(member) =>
                ctx.log.info("Member detected as unreachable: {}", member)
              case ReachableMember(member) =>
                ctx.log.info("Member back to reachable: {}", member)
            }

          case MemberChange(changeEvent) =>
            changeEvent match {
              case MemberUp(member) =>
                ctx.log.info(s"Member is Up: ${member.address} Role: ${member.roles.head}")

              /*
               You can perform some activities after a member is up
               */
              case MemberRemoved(member, previousStatus) =>
                ctx.log.info("Member is Removed: {} after {}", member.address, previousStatus)
              /*
               You can perform some activities after a member is removed
               */
              case _: MemberEvent => // ignore
            }
        }
        Behaviors.same
      }
    }
}
