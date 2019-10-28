package com.aws.activeMq.Example.Spring

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.RedeliveryPolicy
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.jms.annotation.EnableJms
import org.springframework.jms.config.DefaultJmsListenerContainerFactory
import org.springframework.jms.connection.CachingConnectionFactory
import org.springframework.jms.core.JmsTemplate
import org.springframework.jms.listener.DefaultMessageListenerContainer
import org.springframework.jms.support.converter.MappingJackson2MessageConverter
import org.springframework.jms.support.converter.MessageConverter
import org.springframework.jms.support.converter.MessageType
import org.springframework.jms.support.destination.DynamicDestinationResolver
import org.springframework.scheduling.annotation.EnableScheduling
import javax.jms.ConnectionFactory
import SpringBootExample._
//remove if not needed
import scala.collection.JavaConversions._
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jms.activemq.ActiveMQProperties
import org.springframework.boot.context.properties.ConfigurationProperties


object SpringBootExample {

  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[SpringBootExample], args: _*)
  }
}

@SpringBootApplication
@EnableJms
@EnableScheduling
@Configuration
class SpringBootExample {

  @Bean
  @Primary
  def activeMqConnectionFactory(properties: ActiveMQProperties): ConnectionFactory = {
    val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory(properties.getBrokerUrl)
    connectionFactory.getPrefetchPolicy.setQueuePrefetch(1)
    val redeliveryPolicy: RedeliveryPolicy = connectionFactory.getRedeliveryPolicy
    redeliveryPolicy.setUseExponentialBackOff(true)
    redeliveryPolicy.setMaximumRedeliveries(4)
    connectionFactory
  }

  @Bean
  def cachingConnectionFactory(@Qualifier("activeMqConnectionFactory") activeMqConnectionFactory: ConnectionFactory): ConnectionFactory = {
    val cachingConnectionFactory: CachingConnectionFactory = new CachingConnectionFactory(activeMqConnectionFactory)
    cachingConnectionFactory.setCacheConsumers(true)
    cachingConnectionFactory.setCacheProducers(true)
    cachingConnectionFactory.setReconnectOnException(true)
    cachingConnectionFactory
  }

  @Bean
  def jacksonMessageConverter(): MessageConverter = {
    val converter: MappingJackson2MessageConverter =
      new MappingJackson2MessageConverter()
    converter.setTargetType(MessageType.TEXT)
    converter.setTypeIdPropertyName("_messageType")
    converter
  }

  @Bean
  @Primary
  def queueJmsTemplate(@Qualifier("cachingConnectionFactory") connectionFactory: ConnectionFactory): JmsTemplate = {
    val jmsTemplate: JmsTemplate = new JmsTemplate(connectionFactory)
    jmsTemplate.setDestinationResolver(new DynamicDestinationResolver())
    jmsTemplate.setMessageConverter(jacksonMessageConverter())
    jmsTemplate.setPriority(4)
    jmsTemplate.setTimeToLive(30000L)
    jmsTemplate.setExplicitQosEnabled(true)
    jmsTemplate
  }

  @Bean
  def topicJmsTemplate(@Qualifier("cachingConnectionFactory") connectionFactory: ConnectionFactory): JmsTemplate = {
    val jmsTemplate: JmsTemplate = new JmsTemplate(connectionFactory)
    jmsTemplate.setPubSubDomain(true)
    jmsTemplate.setDestinationResolver(new DynamicDestinationResolver())
    jmsTemplate.setMessageConverter(jacksonMessageConverter())
    jmsTemplate
  }

  @Value("${clientId:default_clientId}")
  var clientId: String = _

  @Bean(name = Array("TopicListenerContainerFactory"))
  def exampleListenerContainerFactory(@Qualifier("activeMqConnectionFactory") connectionFactory: ConnectionFactory): DefaultJmsListenerContainerFactory = {
    val factory: DefaultJmsListenerContainerFactory =
      new DefaultJmsListenerContainerFactory()
    factory.setConnectionFactory(connectionFactory)
    factory.setDestinationResolver(new DynamicDestinationResolver())
    factory.setConcurrency("1-1")
    factory.setClientId(clientId)
    factory.setSubscriptionDurable(true)
    factory.setCacheLevel(DefaultMessageListenerContainer.CACHE_AUTO)
    factory.setMessageConverter(jacksonMessageConverter())
    factory.setPubSubDomain(true)
    factory
  }

  @Bean
  @ConfigurationProperties(prefix = "spring.activemq")
  def activeMQProperties(): ActiveMQProperties = new ActiveMQProperties()

}