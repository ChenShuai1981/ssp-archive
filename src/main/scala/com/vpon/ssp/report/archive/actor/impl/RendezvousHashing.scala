package com.vpon.ssp.report.archive.actor.impl

import java.nio.charset.Charset

import akka.actor._
import com.google.common.hash.Hashing

/**
 * Implementation of rendezvous hashing.
 * @param msgstr function to convert message into string
 * @param nodestr function to convert node into string
 * @tparam T type of node
 */
class RendezvousHashing[T](seed: Int, msgstr: Any => String, nodestr: T => String) {

  private[this] val hashFunction = Hashing.murmur3_128(seed)

  /**
   * Calculates hash value for given message and node.
   * @param msg message
   * @param node node
   * @return hash value
   */
  private[this] def hash(msg: String, node: String): Long = {
    val charset = Charset.forName("UTF-8")
    hashFunction.newHasher()
      .putString(msg, charset)
      .putString(node, charset)
      .hash()
      .asLong()
  }

  /**
   * Rendezvouz hashing algorithm. Calculates hash for each (message, node) pair
   * and chooses the node with highest hash value.
   *
   * @param msg message to be sent
   * @param nodes list of nodes to choose from
   * @return `Some` if node found, otherwise `None`
   */
  def nodeFor(msg: Any, nodes: Iterable[T]): Option[T] = {
    if(nodes.isEmpty) {
      None
    } else {
      val hashes = nodes.map { node =>
        (node, hash(msgstr(msg), nodestr(node)))
      }
      val highestHash = hashes.maxBy(_._2)
      Some(highestHash._1)
    }
  }
}

object RendezvousHashing {

  /**
   * Translates given actor address into string.
   * @param adr actor address
   * @return address as string
   */
  private[this] def addressToString(adr: Address) = {
    val hostport = for {
      host <- adr.host
      port <- adr.port
    } yield s"$host:$port"

    // return host:port if those exist, otherwise system
    hostport.getOrElse(adr.system)
  }


  /**
   * Creates new instance of Rendezvous hashing specialized for `ActorSelection`s.
   * @param system `ActorSystem`
   * @return `RendezvousHashing` instance
   */
  def apply(system: ActorSystem): RendezvousHashing[ActorSelection] = {

    val selfAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

    val nodestr: (ActorSelection) => String = _.anchorPath.address match {
      case Address(_, _, None, None) ⇒ addressToString(selfAddress)
      case a ⇒ addressToString(a)
    }

    val seed = -50000
    new RendezvousHashing[ActorSelection](seed, _.toString, nodestr)
  }
}
