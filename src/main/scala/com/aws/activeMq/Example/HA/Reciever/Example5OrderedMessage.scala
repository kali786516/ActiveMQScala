package com.aws.activeMq.Example.HA.Reciever

import com.aws.activeMq.Example.HA.Sender.AbstractExampleApplication
import com.aws.activeMq.Example.HA.Reciever.DelayingMessageListener
import javax.jms._
import java.util.ArrayList
import java.util.List
//remove if not needed
import scala.collection.JavaConversions._

class Example5 extends AbstractExampleApplication {

  private var consumers: List[MessageConsumer] = new ArrayList()

  private var sessions: List[Session] = new ArrayList()

   def start(): Unit = {
    for (x <- 0.until(5)) {
      val session: Session =
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val queue: Queue = session.createQueue("TEST_DESTINATION")
      sessions.add(session)
      val consumer: MessageConsumer = session.createConsumer(queue)
      if (x % 2 == 0) {
        consumer.setMessageListener(
          new DelayingMessageListener(String.valueOf(x) + " Fast", 10))
      } else {
        consumer.setMessageListener(
          new DelayingMessageListener(String.valueOf(x) + " Slow", 100))
      }
      consumers.add(consumer)
    }
    connection.start()
  }

  override
  protected def sendMessages(): Unit = {
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue: Queue = session.createQueue("TEST_DESTINATION")
    messageProducer = session.createProducer(queue)
    for (x <- 0.until(100)) {
      val textMessage: TextMessage = session.createTextMessage("Message " + x)
      if (x % 2 == 0) {
        textMessage.setStringProperty("JMSXGroupID", "Even")
      } else {
        textMessage.setStringProperty("JMSXGroupID", "Odd")
      }
      //textMessage.setStringProperty("JMSXGroupID", orderOrAccountNumber);
      messageProducer.send(textMessage)
    }
    // Could be used for an account number or an order
    // Could be used for an account number or an order
  }

  override def shutdown(): Unit = {
    messageProducer.close()
    for (consumer <- consumers) {
      consumer.close()
    }
    for (session <- sessions) {
      session.close()
    }
    super.shutdown()
  }

}

object Example5OrderedMessage {

  def main(args: Array[String]): Unit = {
    val example: Example5 = new Example5()
    example.start()
  }
}


