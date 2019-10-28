package com.aws.activeMq.Example.HA.Sender

import com.aws.activeMq.Example.HA.Sender.AbstractExampleApplication
//remove if not needed
import scala.collection.JavaConversions._

class MessageSender extends AbstractExampleApplication

object MessageSender {
  def main(args: Array[String]): Unit = {
    //Sending of 100 messages is performed as part of the constructor
    val application = new MessageSender()
    application.sendMessages()
    application.shutdown()
  }
}
