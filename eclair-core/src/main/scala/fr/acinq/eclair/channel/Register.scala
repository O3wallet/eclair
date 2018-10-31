/*
 * Copyright 2018 ACINQ SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.acinq.eclair.channel

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import fr.acinq.bitcoin.BinaryData
import fr.acinq.bitcoin.Crypto.PublicKey
import fr.acinq.eclair.{NodeParams, ShortChannelId}
import fr.acinq.eclair.channel.Register._

import scala.concurrent.duration._
import fr.acinq.eclair.wire.ChannelUpdate

/**
  * Created by PM on 26/01/2016.
  */

class Register(nodeParams: NodeParams) extends Actor with ActorLogging {

  context.system.eventStream.subscribe(self, classOf[ChannelCreated])
  context.system.eventStream.subscribe(self, classOf[ChannelRestored])
  context.system.eventStream.subscribe(self, classOf[ChannelIdAssigned])
  context.system.eventStream.subscribe(self, classOf[ShortChannelIdAssigned])
  context.system.eventStream.subscribe(self, classOf[ChannelSignatureSent])
  context.system.eventStream.subscribe(self, classOf[ChannelStateChanged])

  var pubBalancePeers: Set[PublicKey] = nodeParams.channelsDb.listPublicBalancePeers()

  import scala.concurrent.ExecutionContext.Implicits.global
  context.system.scheduler.schedule(initialDelay = 1.seconds, interval = 10.minutes) {
    pubBalancePeers = nodeParams.channelsDb.listPublicBalancePeers()
  }

  override def receive: Receive = main(Map.empty, Map.empty, Map.empty, Map.empty)

  def main(channels: Map[BinaryData, ActorRef], shortIds: Map[ShortChannelId, BinaryData], channelsTo: Map[BinaryData, PublicKey], localBalances: Map[BinaryData, ChannelBalanceInfo]): Receive = {
    case ChannelCreated(channel, _, remoteNodeId, _, temporaryChannelId) =>
      context.watch(channel)
      context become main(channels + (temporaryChannelId -> channel), shortIds, channelsTo + (temporaryChannelId -> remoteNodeId), localBalances)

    case ChannelRestored(channel, _, remoteNodeId, _, channelId, _) =>
      context.watch(channel)
      context become main(channels + (channelId -> channel), shortIds, channelsTo + (channelId -> remoteNodeId), localBalances)

    case ChannelIdAssigned(channel, remoteNodeId, temporaryChannelId, channelId) =>
      context become main(channels + (channelId -> channel) - temporaryChannelId, shortIds, channelsTo + (channelId -> remoteNodeId) - temporaryChannelId, localBalances)

    case ShortChannelIdAssigned(_, channelId, shortChannelId) =>
      context become main(channels, shortIds + (shortChannelId -> channelId), channelsTo, localBalances)

    case Terminated(actor) if channels.values.toSet.contains(actor) =>
      val channelId = channels.collectFirst { case (id, channelActor) if channelActor == actor => id }.get
      val shortChannelId = shortIds.collectFirst { case (shortId, id) if id == channelId => shortId } getOrElse ShortChannelId(0L)
      context become main(channels - channelId, shortIds - shortChannelId, channelsTo - channelId, localBalances - channelId)

    case ChannelSignatureSent(_, commitments, Some(channelUpdate)) =>
      val localBalances1 = localBalances + (commitments.channelId -> getBalances(commitments, channelUpdate))
      if (pubBalancePeers contains commitments.remoteParams.nodeId) notifyWebsocket(localBalances1)
      context become main(channels, shortIds, channelsTo, localBalances1)

    case ChannelStateChanged(_, _, _, previousState, NORMAL, d: DATA_NORMAL) if previousState != NORMAL =>
      val localBalances1 = localBalances + (d.commitments.channelId -> getBalances(d.commitments, d.channelUpdate))
      context become main(channels, shortIds, channelsTo, localBalances1)
      notifyWebsocket(localBalances1)

    case ChannelStateChanged(_, _, _, NORMAL, currentState, hasCommitments: HasCommitments) if currentState != NORMAL =>
      val localBalances1 = localBalances - hasCommitments.channelId
      context become main(channels, shortIds, channelsTo, localBalances1)
      notifyWebsocket(localBalances1)

    case 'channels => sender ! channels

    case 'shortIds => sender ! shortIds

    case 'channelsTo => sender ! channelsTo

    case 'localBalances => sender ! localBalances.values

    case fwd@Forward(channelId, msg) =>
      channels.get(channelId) match {
        case Some(channel) => channel forward msg
        case None => sender ! Failure(ForwardFailure(fwd))
      }

    case fwd@ForwardShortId(shortChannelId, msg) =>
      shortIds.get(shortChannelId).flatMap(channels.get) match {
        case Some(channel) => channel forward msg
        case None => sender ! Failure(ForwardShortIdFailure(fwd))
      }
  }

  private def getBalances(cs: Commitments, cu: ChannelUpdate) = {
    val latestRemoteCommit = cs.remoteNextCommitInfo.left.toOption.map(_.nextRemoteCommit).getOrElse(cs.remoteCommit)
    val canReceiveWithReserve = cs.localCommit.spec.toRemoteMsat - cs.localParams.channelReserveSatoshis * 1000L
    val canSendWithReserve = latestRemoteCommit.spec.toRemoteMsat - cs.remoteParams.channelReserveSatoshis * 1000L

    ChannelBalanceInfo(ChannelBalance(canSendWithReserve, canReceiveWithReserve), cs.remoteParams.nodeId,
      cu.shortChannelId.toLong, cu.cltvExpiryDelta, cu.htlcMinimumMsat, cu.feeBaseMsat,
      cu.feeProportionalMillionths)
  }

  private def notifyWebsocket(localBalances: Map[BinaryData, ChannelBalanceInfo]) = {
    val balances = localBalances.values.filter(pubBalancePeers contains _.peerNodeId)
    context.system.eventStream publish ChannelBalances(balances.toSet)
  }
}

object Register {

  // @formatter:off
  case class Forward[T](channelId: BinaryData, message: T)
  case class ForwardShortId[T](shortChannelId: ShortChannelId, message: T)

  case class ForwardFailure[T](fwd: Forward[T]) extends RuntimeException(s"channel ${fwd.channelId} not found")
  case class ForwardShortIdFailure[T](fwd: ForwardShortId[T]) extends RuntimeException(s"channel ${fwd.shortChannelId} not found")
  // @formatter:on
}

case class ChannelBalance(canSendMsat: Long, canReceiveMsat: Long)

case class ChannelBalanceInfo(balance: ChannelBalance, peerNodeId: PublicKey, shortChannelId: Long,
                              cltvExpiryDelta: Int, htlcMinimumMsat: Long, feeBaseMsat: Long,
                              feeProportionalMillionths: Long)

case class ChannelBalances(localBalances: Set[ChannelBalanceInfo], tag: String = "ChannelBalances")