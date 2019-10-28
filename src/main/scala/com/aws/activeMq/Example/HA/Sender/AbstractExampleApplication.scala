package com.aws.activeMq.Example.HA.Sender

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.ActiveMQPrefetchPolicy
import org.apache.activemq.RedeliveryPolicy
import javax.jms._
//remove if not needed
import scala.collection.JavaConversions._

abstract class AbstractExampleApplication {
  try {
    connect()
    sendMessages()
  } catch {
    case e: JMSException => {
      e.printStackTrace()
      System.exit(1)
    }

  }

  addShutdownHook()
  protected var connectionFactory: ConnectionFactory = _
  protected var connection: Connection = _
  protected var session: Session = _
  protected var messageProducer: MessageProducer = _
  private var queue: Queue = _

  protected def connect(): Unit = {
    connectionFactory = createConnectionFactory()
    connection = connectionFactory.createConnection()
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    queue = session.createQueue("TEST_DESTINATION")
    messageProducer = session.createProducer(queue)
  }

  protected def sendMessages(): Unit = {
    for (x <- 0.until(100)) {
      messageProducer.send(session.createTextMessage("Message " + x))
    }
  }

  protected def shutdown(): Unit = {
    if (null != messageProducer) {
      messageProducer.close()
    }
    connection.close()
    session.close()
  }

  protected def createConnectionFactory(): ConnectionFactory = {
    val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616")
    val prefetchPolicy: ActiveMQPrefetchPolicy = new ActiveMQPrefetchPolicy()
    prefetchPolicy.setQueuePrefetch(1)
    val redeliveryPolicy: RedeliveryPolicy = new RedeliveryPolicy()
    redeliveryPolicy.setMaximumRedeliveries(4)
    redeliveryPolicy.setBackOffMultiplier(2)
    redeliveryPolicy.setInitialRedeliveryDelay(200)
    redeliveryPolicy.setUseExponentialBackOff(true)
    connectionFactory.setRedeliveryPolicy(redeliveryPolicy)
    connectionFactory.setPrefetchPolicy(prefetchPolicy)
    connectionFactory
  }

  def addShutdownHook(): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        super.run()
        try shutdown()
        catch {
          case e: JMSException => e.printStackTrace()

        }
      }
    })
  }

}
