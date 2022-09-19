package akka.cluster

object Implicits {

  implicit class MemberOps(val member: Member) extends AnyVal {

    /*
    Member.ageOrdering uses
    if (upNumber == other.upNumber) Member.addressOrdering.compare(address, other.address) < 0 else upNumber < other.upNumber
     */
    def details: String = s"${member.uniqueAddress}:${member.upNumber}"
    // `upNumber` is monotonically growing sequence number which increases each time new incarnation of the process starts.
  }

  implicit class VectorClockOps(val vc: akka.cluster.VectorClock) extends AnyVal {
    def internals(): String = vc.versions.mkString(",")
  }
}
