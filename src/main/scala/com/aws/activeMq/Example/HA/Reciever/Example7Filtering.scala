package com.aws.activeMq.Example.HA.Reciever

import com.aws.activeMq.Example.HA.Sender.AbstractExampleApplication
import com.aws.activeMq.Example.HA.Reciever.DelayingMessageListener
import javax.jms._
import Example7._
//remove if not needed
import scala.collection.JavaConversions._

class Example7 extends AbstractExampleApplication {

  private var messageConsumer: MessageConsumer = _

  private def start(): Unit = {
    val messageListener: MessageListener =
      new DelayingMessageListener("Consumer", 100)
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue: Queue = session.createQueue("TEST_DESTINATION")
    messageConsumer = session.createConsumer(queue, "type = 'CreateOrder'")
    messageConsumer.setMessageListener(messageListener)
    connection.start()
  }

  protected override def sendMessages(): Unit = {
    val queue: Queue = session.createQueue("TEST_DESTINATION")
    messageProducer = session.createProducer(queue)
    val createOrderMessage: TextMessage =
      session.createTextMessage("Create Order Message")
    createOrderMessage.setStringProperty("type", "CreateOrder")
    messageProducer.send(createOrderMessage)
    val updateOrderMessage: TextMessage =
      session.createTextMessage("Update Order Message")
    updateOrderMessage.setStringProperty("type", "UpdateOrder")
    messageProducer.send(updateOrderMessage)
  }

  override def shutdown(): Unit = {
    messageConsumer.close()
    session.close()
    super.shutdown()
  }

}

object Example7 {

  def main(args: Array[String]): Unit = {
    val example: Example7 = new Example7()
    example.start()
  }

}
