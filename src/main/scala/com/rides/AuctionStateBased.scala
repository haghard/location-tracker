package com.rides

import akka.cluster.UniqueAddress

import java.time.Instant

object AuctionStateBased {

  type MoneyAmount = BigDecimal
  final case class Bid(bidder: String, offer: MoneyAmount, timestamp: Instant, originReplica: UniqueAddress)

  sealed trait Event
  final case class BidRegistered(bid: Bid)                   extends Event
  final case class AuctionFinished(atReplica: UniqueAddress) extends Event
  final case class WinnerDecided(atReplica: UniqueAddress, winningBid: Bid, highestCounterOffer: MoneyAmount)
      extends Event

  sealed trait AuctionPhase
  case object Running extends AuctionPhase
  // one particular replica has seen all other replicas have had the auction finished (Convergence)
  final case class Closing(finishedAtReplica: Set[UniqueAddress]) extends AuctionPhase
  final case object Closed                                        extends AuctionPhase
  final case class AuctionState(phase: AuctionPhase, highestBid: Bid, highestCounterOffer: MoneyAmount) { self =>

    def applyEvent(event: Event): AuctionState =
      event match {
        case BidRegistered(b) =>
          if (isHigherBid(b, highestBid)) withNewHighestBid(b) else withTooLowBid(b)
        case AuctionFinished(atDc) =>
          phase match {
            case Running =>
              self.copy(phase = Closing(Set(atDc)))
            case Closing(alreadyFinishedDcs) =>
              self.copy(phase = Closing(alreadyFinishedDcs + atDc))
            case _ =>
              self
          }
        case _: WinnerDecided =>
          self.copy(phase = Closed)
      }

    def withNewHighestBid(bid: Bid): AuctionState = {
      require(phase != Closed)
      require(isHigherBid(bid, highestBid))
      self.copy(
        highestBid = bid,
        highestCounterOffer = highestBid.offer // keep last highest bid around
      )
    }

    def withTooLowBid(bid: Bid): AuctionState = {
      require(phase != Closed)
      require(isHigherBid(highestBid, bid))
      self.copy(highestCounterOffer = highestCounterOffer.max(bid.offer)) // update highest counter offer
    }

    def isHigherBid(first: Bid, second: Bid): Boolean =
      first.offer > second.offer ||
        (first.offer == second.offer && first.timestamp.isBefore(second.timestamp)) || // if equal, first one wins
        // If timestamps are equal, choose by dc where the offer was submitted
        // In real auctions, this last comparison should be deterministic but unpredictable, so that submitting to a
        // particular replica would not be an advantage.
        (first.offer == second.offer && first.timestamp.equals(second.timestamp)
          && first.originReplica.compareTo(second.originReplica) < 0)
  }

}
