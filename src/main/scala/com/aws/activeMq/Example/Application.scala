package com.aws.activeMq.Example

import org.apache.activemq.ActiveMQConnectionFactory

import org.apache.activemq.ActiveMQXAConnectionFactory

import javax.jms._

import scala.collection.JavaConversions._

class Application {

  //XA is for transaction

  def createConnectionFactory(): ConnectionFactory =
    new ActiveMQConnectionFactory("tcp://localhost:61616")

  def createXAConnectionFactory(): XAConnectionFactory =
    new ActiveMQXAConnectionFactory("tcp://localhost:61616")

  def createQueueConnectionFactory(): QueueConnectionFactory =
    new ActiveMQConnectionFactory("tcp://localhost:61616")

  def createXAQueueConnectionFactory(): XAQueueConnectionFactory =
    new ActiveMQXAConnectionFactory("tcp://localhost:61616")

  def createTopicConnectionFactory(): TopicConnectionFactory =
    new ActiveMQConnectionFactory("tcp://localhost:61616")

  def createXATopicConnectionFactory(): XATopicConnectionFactory =
    new ActiveMQXAConnectionFactory("tcp://localhost:61616")

  def createConnection(cf: ConnectionFactory): Connection =
    cf.createConnection()

  def createXAConnection(cf: XAConnectionFactory): XAConnection =
    cf.createXAConnection()

  def createQueueConnection(cf: QueueConnectionFactory): QueueConnection =
    cf.createQueueConnection()

  def createXAQueueConnection(
                               cf: XAQueueConnectionFactory): XAQueueConnection =
    cf.createXAQueueConnection()

  def createTopicConnection(cf: TopicConnectionFactory): TopicConnection =
    cf.createTopicConnection()

  def createXATopicConnection(
                               cf: XATopicConnectionFactory): XATopicConnection =
    cf.createXATopicConnection()

  def createSession(connection: Connection): Session ={
    //false means transaction
    //true means no transaction
    connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
  }

  def createXASession(connection: XAConnection): XASession =
    connection.createXASession()

  def createQueueSession(connection: QueueConnection): QueueSession =
    connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE)

  def createXAQueueSession(connection: XAQueueConnection): XAQueueSession =
    connection.createXAQueueSession()

  def createTopicSession(connection: TopicConnection): TopicSession =
    connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE)

  def createXATopicSession(connection: XATopicConnection): XATopicSession =
    connection.createXATopicSession()

  def sendTextMessageToQueue(message: String, session: Session): Unit = {
    val queue: Queue = session.createQueue("TEST_DESTINATION")
    val msg: TextMessage = session.createTextMessage(message)
    val messageProducer: MessageProducer = session.createProducer(queue)
    messageProducer.send(msg)
  }

  def sendTextMessageToQueue(message: String, session: QueueSession): Unit = {
    val queue: Queue = session.createQueue("TEST_DESTINATION")
    val msg: TextMessage = session.createTextMessage(message)
    val messageProducer: QueueSender = session.createSender(queue)
    messageProducer.send(msg)
  }

  def sendTextMessageToTopic(message: String, session: Session): Unit = {
    val queue: Topic = session.createTopic("TEST_TOPIC")
    val msg: TextMessage = session.createTextMessage(message)
    val messageProducer: MessageProducer = session.createProducer(queue)
    //0-9, 9 highest, all messages, 4 default
    messageProducer.setPriority(9)
    //milliseconds, 0 default - doesn't expire
    messageProducer.setTimeToLive(10000)
    messageProducer.send(msg, DeliveryMode.NON_PERSISTENT, 9, 20000)
  }

  def consumeFromQueue(session: Session,
                       destination: String,
                       messageListener: MessageListener): MessageConsumer = {
    val queue: Queue = session.createQueue(destination)
    val consumer: MessageConsumer = session.createConsumer(queue)
    consumer.setMessageListener(messageListener)
    consumer
  }

  def consumeFromTopic(session: Session,
                       destination: String,
                       messageListener: MessageListener): TopicSubscriber = {
    val topic: Topic = session.createTopic(destination)
    val consumer: TopicSubscriber =
      session.createDurableSubscriber(topic, "test-subscription")
    consumer.setMessageListener(messageListener)
    consumer
  }

  def consumerFromQDestination(session:Session,destination:String) = {
    val queue: Queue = session.createQueue(destination)
    val consumer: MessageConsumer = session.createConsumer(queue)
    val someCondition:Boolean = true;
    while (someCondition) {
      val message:Message = consumer.receive(500);
      if (null != message) {
        // do something with message
        println("Do something with message")
        println(message)
      }
    }
  }

  def consumerFromQDestination2(session:Session,destination:String,messageListener: MessageListener):MessageConsumer = {
    val queue: Queue = session.createQueue(destination)
    val consumer: MessageConsumer = session.createConsumer(queue)
    consumer.setMessageListener(messageListener);
    return consumer
  }

  def consumerFromTopicDestination(session:Session,destination:String,messageListener: MessageListener):MessageConsumer = {
    val topic: Topic = session.createTopic(destination)
    val consumer: MessageConsumer = session.createConsumer(topic)
    consumer.setMessageListener(messageListener);
    return consumer
  }

  //durable subscription is to get messages from topic or Q when consumer is off (stopped)

  def consumerFromTopicDurableSubscription(session:Session,destination:String,messageListener: MessageListener):TopicSubscriber = {
    val topic: Topic = session.createTopic(destination)
    val consumer: TopicSubscriber = session.createDurableSubscriber(topic,"test-subscription")
    consumer.setMessageListener(messageListener);
    return consumer
  }


}

object Application {
  def main(args: Array[String]): Unit = {

    /*send message to Q*/

    /*Producer*/
    val producerApp: Application = new Application()
    val producerConnectionFactory: ConnectionFactory = producerApp.createConnectionFactory()
    val ProducerConnection: Connection = producerApp.createConnection(producerConnectionFactory)
    val producerSession: Session = producerApp.createSession(ProducerConnection)
    producerApp.sendTextMessageToQueue("TestMessage786",producerSession)
    producerSession.close()
    ProducerConnection.close()

    /*send message to Topic*/
    /*Producer*/
    val producerAppTopic: Application = new Application()
    val producerConnectionFactoryTopic: ConnectionFactory = producerAppTopic.createConnectionFactory()
    val ProducerConnectionTopic: Connection = producerAppTopic.createConnection(producerConnectionFactory)
    val producerSessionTopic: Session = producerAppTopic.createSession(ProducerConnectionTopic)
    producerAppTopic.sendTextMessageToTopic("TestMessage786123",producerSession)
    producerSessionTopic.close()
    ProducerConnectionTopic.close()


    /*
    Queue Connection
    val app: Application = new Application()
    val cf: QueueConnectionFactory = app.createQueueConnectionFactory()
    val conn: QueueConnection = app.createQueueConnection(cf)
    val session: Session = app.createQueueSession(conn)
    app.sendTextMessageToTopic("Another Message",session)
    session.close()
    conn.close()
     */

    /*Consumer*/
    /*consume from Q*/
    val consumerQApp: Application = new Application()
    val consumerQConnectionFactory: ConnectionFactory = consumerQApp.createConnectionFactory()
    val consumerQConn: Connection = consumerQApp.createConnection(consumerQConnectionFactory)
    val consumerQSession: Session = consumerQApp.createSession(consumerQConn)
    val messageListenerValue:MessageListener = new MessageListener {
      override def onMessage(message: Message): Unit = println("From Q :- " + message)
    }
    val consumerQ:MessageConsumer = consumerQApp.consumerFromQDestination2(consumerQSession,"TEST_DESTINATION", messageListenerValue)
    consumerQConn.start()

    Runtime.getRuntime.addShutdownHook(new Thread(){
      override
      def run(): Unit = {
        try {
          super.run()
          consumerQConn.stop()
          consumerQ.close()
          consumerQSession.close()
          consumerQConn.stop()
        } catch {
        case e: Exception => println(e)
        }
      }
    })

    /*Consumer*/
    /*consume from Topic*/
    val consumerTApp: Application = new Application()
    val consumerTConnectionFactory: ConnectionFactory = consumerTApp.createConnectionFactory()
    val consumerTConn: Connection = consumerTApp.createConnection(consumerTConnectionFactory)
    val consumerTSession: Session = consumerTApp.createSession(consumerTConn)
    val messageListenerValueT:MessageListener = new MessageListener {
      override def onMessage(message: Message): Unit = println("From Topic :- " +message)
    }
    val consumerT:MessageConsumer = consumerTApp.consumerFromTopicDestination(consumerQSession,"TEST_TOPIC", messageListenerValueT)
    consumerQConn.start()

    Runtime.getRuntime.addShutdownHook(new Thread(){
      override
      def run(): Unit = {
        try {
          super.run()
          consumerTConn.stop()
          consumerT.close()
          consumerTSession.close()
          consumerTConn.stop()
        } catch {
          case e: Exception => println(e)
        }
      }
    })


    /*Consumer*/
    /*consume from Topic using Durable Subscription*/
    //durable subscription is to get messages from topic or Q when consumer is off (stopped)
    val consumerTAppDS: Application = new Application()
    val consumerTConnectionFactoryDS: ConnectionFactory = consumerTAppDS.createConnectionFactory()
    val consumerTConnDS: Connection = consumerTAppDS.createConnection(consumerTConnectionFactoryDS)
    val consumerTSessionDS: Session = consumerTAppDS.createSession(consumerTConn)
    consumerTConnDS.setClientID("MyUniqueClientId")
    val messageListenerValueTDS:MessageListener = new MessageListener {
      override def onMessage(message: Message): Unit = println("From Topic :- " +message)
    }
    val consumerTDS:TopicSubscriber = consumerTAppDS.consumerFromTopicDurableSubscription(consumerQSession,"TEST_TOPIC", messageListenerValueTDS)
    consumerTConnDS.start()

    Runtime.getRuntime.addShutdownHook(new Thread(){
      override
      def run(): Unit = {
        try {
          super.run()
          consumerTConnDS.stop()
          consumerTDS.close()
          consumerTSessionDS.close()
          consumerTConnDS.stop()
          // if you are finished with subscrioption
          consumerTSessionDS.unsubscribe("test-subscription")
        } catch {
          case e: Exception => println(e)
        }
      }
    })



    /*
    /*send message to TOPIC*/
    val app: Application = new Application()
    val cf: ConnectionFactory = app.createConnectionFactory()
    val conn: Connection = app.createConnection(cf)
    val session: Session = app.createSession(conn)
    app.sendTextMessageToTopic("TestMessage",session)
    session.close()
    conn.close()
    */



  }
}
