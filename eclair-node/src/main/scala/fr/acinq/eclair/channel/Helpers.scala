package fr.acinq.eclair.channel

import fr.acinq.bitcoin.{OutPoint, _}
import fr.acinq.eclair.crypto.Generators
import fr.acinq.eclair.transactions.Common._
import fr.acinq.eclair.transactions._
import fr.acinq.eclair.wire.UpdateFulfillHtlc

import scala.util.Try

/**
  * Created by PM on 20/05/2016.
  */

object Helpers {

  object Funding {

    /**
      * Extracts a [TxIn] from a funding tx id and an output index
      * @param fundingTxId
      * @param fundingTxOutputIndex
      */
    def inputFromFundingTx(fundingTxId: BinaryData, fundingTxOutputIndex: Int): TxIn = TxIn(OutPoint(fundingTxId, fundingTxOutputIndex), Array.emptyByteArray, 0xffffffffL)

    /**
      * Creates both sides's first commitment transaction
      * @param funder
      * @param params
      * @param pushMsat
      * @param fundingTxHash
      * @param fundingTxOutputIndex
      * @param remoteFirstPerCommitmentPoint
      * @return (localSpec, localTx, remoteSpec, remoteTx, fundingTxOutput)
      */
    def makeFirstCommitmentTx(funder: Boolean, params: ChannelParams, pushMsat: Long, fundingTxHash: BinaryData, fundingTxOutputIndex: Int, remoteFirstPerCommitmentPoint: BinaryData): (CommitmentSpec, Transaction, CommitmentSpec, Transaction, TxOut) = {
      val toLocalMsat = if (funder) params.fundingSatoshis * 1000 - pushMsat else pushMsat
      val toRemoteMsat = if (funder) pushMsat else params.fundingSatoshis * 1000 - pushMsat

      // local and remote feerate are the same at this point (funder gets to choose the initial feerate)
      val localSpec = CommitmentSpec(Set.empty[Htlc], feeRate = params.localParams.feeratePerKw, to_local_msat = toLocalMsat, to_remote_msat = toRemoteMsat)
      val remoteSpec = CommitmentSpec(Set.empty[Htlc], feeRate = params.remoteParams.feeratePerKw, to_local_msat = toRemoteMsat, to_remote_msat = toLocalMsat)

      val commitmentInput = Funding.inputFromFundingTx(fundingTxHash, fundingTxOutputIndex)
      val localPerCommitmentPoint = Generators.perCommitPoint(params.localParams.shaSeed, 0)
      val localTx = CommitmentSpec.makeLocalTxTemplate(params.localParams, params.remoteParams, commitmentInput :: Nil, localPerCommitmentPoint.data, localSpec).makeTx
      val remoteTx = CommitmentSpec.makeRemoteTxTemplate(params.localParams, params.remoteParams, commitmentInput :: Nil, remoteFirstPerCommitmentPoint, remoteSpec).makeTx

      val localFundingPubkey = params.localParams.fundingPrivkey.point
      val fundingTxOutput = TxOut(Satoshi(params.fundingSatoshis), publicKeyScript = Common.anchorPubkeyScript(localFundingPubkey, params.remoteParams.fundingPubkey))

      (localSpec, localTx, remoteSpec, remoteTx, fundingTxOutput)
    }

  }

  object Closing {

    def checkCloseSignature(closeSig: BinaryData, closeFee: Satoshi, d: DATA_NEGOTIATING): Try[Transaction] = ???

    /*{
    val (finalTx, ourCloseSig) = Helpers.makeFinalTx(d.commitments, d.ourShutdown.scriptPubkey, d.theirShutdown.scriptPubkey, closeFee)
    val signedTx = addSigs(d.commitments.ourParams, d.commitments.theirParams, d.commitments.anchorOutput.amount, finalTx, ourCloseSig.sig, closeSig)
    checksig(d.commitments.ourParams, d.commitments.theirParams, d.commitments.anchorOutput, signedTx).map(_ => signedTx)
  }*/

    /**
      *
      * @param commitments
      * @param ourScriptPubKey
      * @param theirScriptPubKey
      * @param closeFee bitcoin fee for the final tx
      * @return a (final tx, fee, our signature) tuple. The tx is not signed.
      */
    def makeFinalTx(commitments: Commitments, ourScriptPubKey: BinaryData, theirScriptPubKey: BinaryData, closeFee: Satoshi): (Transaction, Long, BinaryData) = ???

    /*{
    val amount_us = Satoshi(commitments.ourCommit.spec.amount_us_msat / 1000)
    val amount_them = Satoshi(commitments.theirCommit.spec.amount_us_msat / 1000)
    val finalTx = Scripts.makeFinalTx(commitments.ourCommit.publishableTx.txIn, ourScriptPubKey, theirScriptPubKey, amount_us, amount_them, closeFee)
    val ourSig = Helpers.sign(commitments.ourParams, commitments.theirParams, commitments.anchorOutput.amount, finalTx)
    (finalTx, ClosingSigned(closeFee.toLong, ourSig))
  }*/

    /**
      *
      * @param commitments
      * @param ourScriptPubKey
      * @param theirScriptPubKey
      * @return a (final tx, fee, our signature) tuple. The tx is not signed. Bitcoin fees will be copied from our
      *         last commit tx
      */
    def makeFinalTx(commitments: Commitments, ourScriptPubKey: BinaryData, theirScriptPubKey: BinaryData): (Transaction, Long, BinaryData) = {
      val commitFee = commitments.fundingTxOutput.amount.toLong - commitments.localCommit.publishableTx.txOut.map(_.amount.toLong).sum
      val closeFee = Satoshi(2 * (commitFee / 4))
      makeFinalTx(commitments, ourScriptPubKey, theirScriptPubKey, closeFee)
    }

    /**
      * Claim a revoked commit tx using the matching revocation preimage, which allows us to claim all its inputs without a
      * delay
      *
      * @param theirTxTemplate    revoked commit tx template
      * @param revocationPreimage revocation preimage (which must match this specific commit tx)
      * @param privateKey         private key to send the claimed funds to (the returned tx will include a single P2WPKH output)
      * @return a signed transaction that spends the revoked commit tx
      */
    def claimRevokedCommitTx(theirTxTemplate: CommitTxTemplate, revocationPreimage: BinaryData, privateKey: BinaryData): Transaction = {
      val theirTx = theirTxTemplate.makeTx
      val outputs = collection.mutable.ListBuffer.empty[TxOut]

      // first, find out how much we can claim
      val outputsToClaim = (theirTxTemplate.localOutput.toSeq ++ theirTxTemplate.htlcReceivedOutputs ++ theirTxTemplate.htlcOfferedOutputs).filter(o => theirTx.txOut.indexOf(o.txOut) != -1)
      val totalAmount = outputsToClaim.map(_.amount).sum // TODO: substract a small network fee

      // create a tx that sends everything to our private key
      val tx = Transaction(version = 2,
        txIn = Seq.empty[TxIn],
        txOut = TxOut(totalAmount, pay2wpkh(Crypto.publicKeyFromPrivateKey(privateKey))) :: Nil,
        lockTime = 0)

      // create tx inputs that spend each output that we can spend
      val inputs = outputsToClaim.map(outputTemplate => {
        val index = theirTx.txOut.indexOf(outputTemplate.txOut)
        TxIn(OutPoint(theirTx, index), signatureScript = BinaryData.empty, sequence = 0xffffffffL)
      })
      assert(inputs.length == outputsToClaim.length)

      // and sign them
      val tx1 = tx.copy(txIn = inputs)
      val witnesses = for (i <- 0 until tx1.txIn.length) yield {
        val sig = Transaction.signInput(tx1, i, outputsToClaim(i).redeemScript, SIGHASH_ALL, outputsToClaim(i).amount, 1, privateKey)
        val witness = ScriptWitness(sig :: revocationPreimage :: outputsToClaim(i).redeemScript :: Nil)
        witness
      }

      tx1.updateWitnesses(witnesses)
    }

    /**
      * claim an HTLC that we received using its payment preimage. This is used only when the other party publishes its
      * current commit tx which contains pending HTLCs.
      *
      * @param tx              commit tx published by the other party
      * @param htlcTemplate    HTLC template for an HTLC in the commit tx for which we have the preimage
      * @param paymentPreimage HTLC preimage
      * @param privateKey      private key which matches the pubkey  that the HTLC was sent to
      * @return a signed transaction that spends the HTLC in their published commit tx.
      *         This tx is not spendable right away: it has both an absolute CLTV time-out and a relative CSV time-out
      *         before which it can be published
      */
    def claimReceivedHtlc(tx: Transaction, htlcTemplate: ReceivedHTLCOutputTemplate, paymentPreimage: BinaryData, privateKey: BinaryData): Transaction = ???
    /*{
      require(htlcTemplate.htlc.add.paymentHash == BinaryData(Crypto.sha256(paymentPreimage)), "invalid payment preimage")
      // find its index in their tx
      val index = tx.txOut.indexOf(htlcTemplate.txOut)

      val tx1 = Transaction(version = 2,
        txIn = TxIn(OutPoint(tx, index), BinaryData.empty, sequence = Common.toSelfDelay2csv(htlcTemplate.delay)) :: Nil,
        txOut = TxOut(htlcTemplate.amount, Common.pay2pkh(Crypto.publicKeyFromPrivateKey(privateKey))) :: Nil,
        lockTime = ??? /*Scripts.locktime2long_cltv(htlcTemplate.htlc.add.expiry)*/)

      val sig = Transaction.signInput(tx1, 0, htlcTemplate.redeemScript, SIGHASH_ALL, htlcTemplate.amount, 1, privateKey)
      val witness = ScriptWitness(sig :: paymentPreimage :: htlcTemplate.redeemScript :: Nil)
      val tx2 = tx1.updateWitness(0, witness)
      tx2
    }*/

    /**
      * claim all the HTLCs that we've received from their current commit tx
      *
      * @param txTemplate  commit tx published by the other party
      * @param commitments our commitment data, which include payment preimages
      * @return a list of transactions (one per HTLC that we can claim)
      */
    def claimReceivedHtlcs(tx: Transaction, txTemplate: CommitTxTemplate, commitments: Commitments): Seq[Transaction] = ???
    /*{
      val preImages = commitments.localChanges.all.collect { case UpdateFulfillHtlc(_, id, paymentPreimage) => paymentPreimage }
      // TODO: FIXME !!!
      //val htlcTemplates = txTemplate.htlcSent
      val htlcTemplates = txTemplate.htlcReceived ++ txTemplate.htlcSent

      //@tailrec
      def loop(htlcs: Seq[HTLCTemplate], acc: Seq[Transaction] = Seq.empty[Transaction]): Seq[Transaction] = Nil

      /*{
      htlcs.headOption match {
        case Some(head) =>
          preImages.find(preImage => head.htlc.add.rHash == bin2sha256(Crypto.sha256(preImage))) match {
            case Some(preImage) => loop(htlcs.tail, claimReceivedHtlc(tx, head, preImage, commitments.ourParams.finalPrivKey) +: acc)
            case None => loop(htlcs.tail, acc)
          }
        case None => acc
      }
    }*/
      loop(htlcTemplates)
    }*/

    def claimSentHtlc(tx: Transaction, htlcTemplate: OfferedHTLCOutputTemplate, privateKey: BinaryData): Transaction = ???
    /*{
      val index = tx.txOut.indexOf(htlcTemplate.txOut)
      val tx1 = Transaction(
        version = 2,
        txIn = TxIn(OutPoint(tx, index), Array.emptyByteArray, sequence = Common.toSelfDelay2csv(htlcTemplate.delay)) :: Nil,
        txOut = TxOut(htlcTemplate.amount, Common.pay2pkh(Crypto.publicKeyFromPrivateKey(privateKey))) :: Nil,
        lockTime = ??? /*Scripts.locktime2long_cltv(htlcTemplate.htlc.add.expiry)*/)

      val sig = Transaction.signInput(tx1, 0, htlcTemplate.redeemScript, SIGHASH_ALL, htlcTemplate.amount, 1, privateKey)
      val witness = ScriptWitness(sig :: Hash.Zeroes :: htlcTemplate.redeemScript :: Nil)
      tx1.updateWitness(0, witness)
    }*/

    // TODO: fix this!
    def claimSentHtlcs(tx: Transaction, txTemplate: CommitTxTemplate, commitments: Commitments): Seq[Transaction] = Nil

    /*{
    // txTemplate could be our template (we published our commit tx) or their template (they published their commit tx)
    val htlcs1 = txTemplate.htlcSent.filter(_.ourKey == commitments.ourParams.finalPubKey)
    val htlcs2 = txTemplate.htlcReceived.filter(_.theirKey == commitments.ourParams.finalPubKey)
    val htlcs = htlcs1 ++ htlcs2
    htlcs.map(htlcTemplate => claimSentHtlc(tx, htlcTemplate, commitments.ourParams.finalPrivKey))
  }*/
  }

}