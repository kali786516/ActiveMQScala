package com.aws.activeMq.Example.HA.Reciever

import com.aws.activeMq.Example.HA.Sender.AbstractExampleApplication
import com.aws.activeMq.Example.HA.Reciever.DelayingMessageListener
import javax.jms._
import java.util.ArrayList
import java.util.List
import Example2._
//remove if not needed
import scala.collection.JavaConversions._

import org.springframework.jms.listener.DefaultMessageListenerContainer
import javax.jms.JMSException
import Example3._


object Example3 {
  def main(args: Array[String]): Unit = {
    val example: Example3 = new Example3()
    example.start()
  }

}

class Example3 extends AbstractExampleApplication {

  private var container: DefaultMessageListenerContainer = _

  private def start(): Unit = {
    container = new DefaultMessageListenerContainer()
    container.setConnectionFactory(connectionFactory)
    container.setConcurrentConsumers(5)
    container.setCacheLevel(DefaultMessageListenerContainer.CACHE_CONSUMER)
    container.setDestinationName("TEST_DESTINATION")
    container.setMessageListener(new DelayingMessageListener("Default", 10))
    container.setAutoStartup(true)
    container.initialize()
    container.start()
  }

  override def shutdown(): Unit = {
    container.stop()
    super.shutdown()
  }

}