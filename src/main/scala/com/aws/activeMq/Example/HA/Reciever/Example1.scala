package com.aws.activeMq.Example.HA.Reciever

import com.aws.activeMq.Example.HA.Sender.AbstractExampleApplication
import com.aws.activeMq.Example.HA.Reciever.DelayingMessageListener
import javax.jms._
import Example1._
//remove if not needed
import scala.collection.JavaConversions._

object Example1 {
  def main(args: String*): Unit = {
    val example: Example1 = new Example1()
    example.start()
  }
}
class Example1 extends AbstractExampleApplication {

  private var messageConsumer: MessageConsumer = _

  private def start(): Unit = {
    val messageListener: MessageListener =
      new DelayingMessageListener("Consumer", 200)
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue: Queue = session.createQueue("TEST_DESTINATION")
    messageConsumer = session.createConsumer(queue)
    messageConsumer.setMessageListener(messageListener)
    connection.start()
  }

  override def shutdown(): Unit = {
    messageConsumer.close()
    session.close()
    super.shutdown()
  }

  protected override def sendMessages(): Unit = {}
  // Do nothing
  // Do nothing

}



