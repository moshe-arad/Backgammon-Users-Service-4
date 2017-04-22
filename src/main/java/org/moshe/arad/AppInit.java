package org.moshe.arad;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.commands.CreateNewUserCommandConsumer;
import org.moshe.arad.kafka.consumers.config.CreateNewUserCommandConfig;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.producers.SimpleBackgammonEventsProducer;
import org.moshe.arad.kafka.producers.config.NewUserCreatedEventConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class AppInit implements ApplicationContextAware {
	
//	@Autowired
//	private CreateNewUserCommandConsumer createNewUserCommandConsumer;
//
//	@Autowired
//	private SimpleProducer simpleProducer;
	
	private ApplicationContext context;
	
	private ExecutorService executor = Executors.newFixedThreadPool(4);
	
	@Autowired
	private CreateNewUserCommandConsumer createNewUserCommandConsumer;
	
	@Autowired
	private CreateNewUserCommandConfig createNewUserCommandConfig;
	
	@Autowired
	private SimpleBackgammonEventsProducer<NewUserCreatedEvent> newUserCreatedEventsProducer;
	
	@Autowired
	private NewUserCreatedEventConfig newUserCreatedEventConfig;
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	public AppInit() {
	}
	
	public void acceptNewUsers(){
		logger.info("Accept new users... start");
		
		ConsumerToProducerQueue consumerToProducerQueue = context.getBean(ConsumerToProducerQueue.class);
		
		createNewUserCommandConsumer.setTopic(KafkaUtils.CREATE_NEW_USER_COMMAND_TOPIC);
		createNewUserCommandConsumer.setSimpleConsumerConfig(createNewUserCommandConfig);
		createNewUserCommandConsumer.initConsumer();
		createNewUserCommandConsumer.setConsumerToProducerQueue(consumerToProducerQueue);
		executor.execute(createNewUserCommandConsumer);
		
		newUserCreatedEventsProducer.setTopic(KafkaUtils.NEW_USER_CREATED_EVENT_TOPIC);
		newUserCreatedEventsProducer.setSimpleProducerConfig(newUserCreatedEventConfig);
		newUserCreatedEventsProducer.setConsumerToProducerQueue(consumerToProducerQueue);
		executor.execute(newUserCreatedEventsProducer);
		
		logger.info("Accept new users... end");
	}
	
	public void shutdown(){
		createNewUserCommandConsumer.setRunning(false);
		createNewUserCommandConsumer.getScheduledExecutor().shutdown();
		
		newUserCreatedEventsProducer.setRunning(false);
		newUserCreatedEventsProducer.getScheduledExecutor().shutdown();

		this.executor.shutdown();
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}
}
