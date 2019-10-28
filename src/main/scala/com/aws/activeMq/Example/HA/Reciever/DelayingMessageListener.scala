package com.aws.activeMq.Example.HA.Reciever

import javax.jms.{JMSException, Message, MessageListener, TextMessage}
//remove if not needed

class DelayingMessageListener(private var id: String, private var delay: Long) extends MessageListener{
  override def onMessage(message: Message): Unit = {
    try {
      val textMessage: TextMessage = message.asInstanceOf[TextMessage]
      println(id + " received : " + textMessage.getText)
      Thread.sleep(delay)
    } catch {
      case e @ (_: JMSException | _: InterruptedException) =>
        e.printStackTrace()

    }
  }
}
