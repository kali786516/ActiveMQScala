package com.aws.activeMq.Example.HA.Reciever

import com.aws.activeMq.Example.HA.Sender.AbstractExampleApplication
import com.aws.activeMq.Example.HA.Reciever.DelayingMessageListener
import javax.jms._
import java.util.ArrayList
import java.util.List
import Example2._
//remove if not needed
import scala.collection.JavaConversions._

object Example2 {
  def main(args: String*): Unit = {
    val example: Example2 = new Example2()
    example.start()
  }
}

class Example2 extends AbstractExampleApplication {

  private var consumers: List[MessageConsumer] = new ArrayList()
  private var sessions: List[Session] = new ArrayList()

  private def start(): Unit = {
    for (x <- 0.until(5)) {
      val session: Session =
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val queue: Queue = session.createQueue("TEST_DESTINATION")
      sessions.add(session)
      val consumer: MessageConsumer = session.createConsumer(queue)
      consumer.setMessageListener(
        new DelayingMessageListener(String.valueOf(x) + " Consumer", 100))
      consumers.add(consumer)
    }
    connection.start()
  }

  override def shutdown(): Unit = {
    for (consumer <- consumers) {
      consumer.close()
    }
    for (session <- sessions) {
      session.close()
    }
    super.shutdown()
  }
}
