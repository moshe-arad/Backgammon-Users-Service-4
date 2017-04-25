package org.moshe.arad.initializer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.commands.CreateNewUserCommandsConsumer;
import org.moshe.arad.kafka.consumers.commands.config.CreateNewUserCommandConfig;
import org.moshe.arad.kafka.consumers.events.json.FromMongoEventsStoreEventConfig;
import org.moshe.arad.kafka.consumers.events.json.FromMongoEventsStoreEventsConsumer;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.producers.commands.json.PullEventsCommandsConfig;
import org.moshe.arad.kafka.producers.commands.json.PullEventsCommandsProducer;
import org.moshe.arad.kafka.producers.events.SimpleBackgammonEventsProducer;
import org.moshe.arad.kafka.producers.events.config.NewUserCreatedEventConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class AppInit implements ApplicationContextAware, AppInitializer {
	
	@Autowired
	private CreateNewUserCommandsConsumer createNewUserCommandConsumer;
	
	@Autowired
	private CreateNewUserCommandConfig createNewUserCommandConfig;
	
	@Autowired
	private SimpleBackgammonEventsProducer<NewUserCreatedEvent> newUserCreatedEventsProducer;
	
	@Autowired
	private NewUserCreatedEventConfig newUserCreatedEventConfig;
	
	@Autowired
	private PullEventsCommandsProducer pullEventsCommandsProducer;
	
	@Autowired
	private PullEventsCommandsConfig pullEventsCommandsConfig;
	
	@Autowired
	private FromMongoEventsStoreEventsConsumer fromMongoEventsStoreEventsConsumer;

	@Autowired
	private FromMongoEventsStoreEventConfig fromMongoEventsStoreEventConfig;
	
	private ApplicationContext context;
	
	private ExecutorService executor = Executors.newFixedThreadPool(4);
	
	private ConsumerToProducerQueue createUserConsumerToProducerQueue;
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	public AppInit() {
		
	}
	
	private void initKafkaCommandsConsumers() {
		logger.info("Initializing create new user command consumer...");
		createNewUserCommandConsumer.setTopic(KafkaUtils.CREATE_NEW_USER_COMMAND_TOPIC);
		createNewUserCommandConsumer.setSimpleConsumerConfig(createNewUserCommandConfig);
		createNewUserCommandConsumer.initConsumer();
		createNewUserCommandConsumer.setConsumerToProducerQueue(createUserConsumerToProducerQueue);
		logger.info("Initialize create new user command consumer, completed...");
	}

	private void initKafkaEventsConsumers() {
		logger.info("Initializing from mongo events store event consumer...");
		fromMongoEventsStoreEventsConsumer.setTopic(KafkaUtils.FROM_MONGO_TO_USERS_SERVICES);
		fromMongoEventsStoreEventsConsumer.setSimpleConsumerConfig(fromMongoEventsStoreEventConfig);
		fromMongoEventsStoreEventsConsumer.initConsumer();
		logger.info("Initialize from mongo events store event consumer, completed...");
	}

	private void initKafkaCommandsProducers() {
		logger.info("Initializing pull events commands producer...");
		pullEventsCommandsProducer.setPeriod(1);
		pullEventsCommandsProducer.setInitialDelay(1);
		pullEventsCommandsProducer.setTopic(KafkaUtils.PULL_EVENTS_COMMAND_TOPIC);
		pullEventsCommandsProducer.setSimpleProducerConfig(pullEventsCommandsConfig);
		logger.info("Initialize pull events commands producer, completed...");
	}

	private void initKafkaEventsProducers() {
		logger.info("Initializing new user created events producer...");
		newUserCreatedEventsProducer.setTopic(KafkaUtils.NEW_USER_CREATED_EVENT_TOPIC);
		newUserCreatedEventsProducer.setSimpleProducerConfig(newUserCreatedEventConfig);
		newUserCreatedEventsProducer.setConsumerToProducerQueue(createUserConsumerToProducerQueue);
		logger.info("Initialize new user created events producer, completed...");
	}

	@Override
	public void startEngine() {
		logger.info("Users Service, Engine is about to start...");
		
		createUserConsumerToProducerQueue = context.getBean(ConsumerToProducerQueue.class);
		
		initKafkaCommandsConsumers();
		initKafkaEventsConsumers();
		initKafkaCommandsProducers();
		initKafkaEventsProducers();
		
		executor.execute(createNewUserCommandConsumer);
		executor.execute(newUserCreatedEventsProducer);
		executor.execute(pullEventsCommandsProducer);
		executor.execute(fromMongoEventsStoreEventsConsumer);
		logger.info("Users Service, Engine started successfuly...");
	}

	@Override
	public void engineShutdown() {
		logger.info("about to do shutdown.");
		createNewUserCommandConsumer.setRunning(false);
		createNewUserCommandConsumer.getScheduledExecutor().shutdown();
		
		newUserCreatedEventsProducer.setRunning(false);
		newUserCreatedEventsProducer.getScheduledExecutor().shutdown();

		this.executor.shutdown();
		logger.info("shutdown compeleted.");
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}	
}
