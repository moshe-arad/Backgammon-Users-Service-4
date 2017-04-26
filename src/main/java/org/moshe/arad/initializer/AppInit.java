package org.moshe.arad.initializer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.ISimpleConsumer;
import org.moshe.arad.kafka.consumers.SimpleConsumerConfig;
import org.moshe.arad.kafka.consumers.commands.CreateNewUserCommandConfig;
import org.moshe.arad.kafka.consumers.commands.CreateNewUserCommandsConsumer;
import org.moshe.arad.kafka.consumers.events.FromMongoEventsStoreEventConfig;
import org.moshe.arad.kafka.consumers.events.FromMongoEventsStoreEventsConsumer;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.producers.ISimpleProducer;
import org.moshe.arad.kafka.producers.SimpleProducerConfig;
import org.moshe.arad.kafka.producers.commands.PullEventsCommandsProducer;
import org.moshe.arad.kafka.producers.events.SimpleEventsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class AppInit implements ApplicationContextAware, IAppInitializer {
	
	@Autowired
	private CreateNewUserCommandsConsumer createNewUserCommandConsumer;
	
	@Autowired
	private CreateNewUserCommandConfig createNewUserCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<NewUserCreatedEvent> newUserCreatedEventsProducer;
	
	@Autowired
	private SimpleProducerConfig newUserCreatedEventConfig;
	
	@Autowired
	private PullEventsCommandsProducer pullEventsCommandsProducer;
	
	@Autowired
	private SimpleProducerConfig pullEventsCommandsConfig;
	
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
	
	@Override
	public void initKafkaCommandsConsumers() {	
		createUserConsumerToProducerQueue = context.getBean(ConsumerToProducerQueue.class);
		
		logger.info("Initializing create new user command consumer...");
		initSingleConsumer(createNewUserCommandConsumer, KafkaUtils.CREATE_NEW_USER_COMMAND_TOPIC, createNewUserCommandConfig, createUserConsumerToProducerQueue);
		logger.info("Initialize create new user command consumer, completed...");
		
		executeProducersAndConsumers(Arrays.asList(createNewUserCommandConsumer));
	}

	@Override
	public void initKafkaEventsConsumers() {
		logger.info("Initializing from mongo events store event consumer...");
		initSingleConsumer(fromMongoEventsStoreEventsConsumer, KafkaUtils.FROM_MONGO_TO_USERS_SERVICES, fromMongoEventsStoreEventConfig, null);
		logger.info("Initialize from mongo events store event consumer, completed...");
		
		executeProducersAndConsumers(Arrays.asList(fromMongoEventsStoreEventsConsumer));
	}

	@Override
	public void initKafkaCommandsProducers() {
		logger.info("Initializing pull events commands producer...");
		initSingleProducer(pullEventsCommandsProducer, 1, 1, TimeUnit.MINUTES, KafkaUtils.PULL_EVENTS_COMMAND_TOPIC, pullEventsCommandsConfig, null);
		logger.info("Initialize pull events commands producer, completed...");
		
		executeProducersAndConsumers(Arrays.asList(pullEventsCommandsProducer));
	}

	@Override
	public void initKafkaEventsProducers() {		
		logger.info("Initializing new user created events producer...");
		initSingleProducer(newUserCreatedEventsProducer, 500, 0, TimeUnit.MILLISECONDS, KafkaUtils.NEW_USER_CREATED_EVENT_TOPIC, newUserCreatedEventConfig, createUserConsumerToProducerQueue);
		logger.info("Initialize new user created events producer, completed...");
		
		executeProducersAndConsumers(Arrays.asList(newUserCreatedEventsProducer));
	}

	@Override
	public void engineShutdown() {
		logger.info("about to do shutdown.");		
		shutdownSingleConsumer(createNewUserCommandConsumer);
		shutdownSingleConsumer(fromMongoEventsStoreEventsConsumer);		
		shutdownSingleProducer(newUserCreatedEventsProducer);
		shutdownSingleProducer(pullEventsCommandsProducer);
		selfShutdown();
		logger.info("shutdown compeleted.");
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}
	
	private void initSingleConsumer(ISimpleConsumer consumer, String topic, SimpleConsumerConfig consumerConfig, ConsumerToProducerQueue queue) {
		consumer.setTopic(topic);
		consumer.setSimpleConsumerConfig(consumerConfig);
		consumer.initConsumer();	
		consumer.setConsumerToProducerQueue(queue);
	}
	
	private void initSingleProducer(ISimpleProducer producer, int period, int initialDelay, TimeUnit timeUnit, String topic, SimpleProducerConfig consumerConfig, ConsumerToProducerQueue queue) {
		producer.setPeriod(period);
		producer.setInitialDelay(initialDelay);
		producer.setTimeUnit(timeUnit);
		producer.setTopic(topic);
		producer.setSimpleProducerConfig(consumerConfig);	
		producer.setConsumerToProducerQueue(queue);
	}
	
	private void shutdownSingleConsumer(ISimpleConsumer consumer) {
		consumer.setRunning(false);
		consumer.getScheduledExecutor().shutdown();	
	}
	
	private void shutdownSingleProducer(ISimpleProducer producer) {
		producer.setRunning(false);
		producer.getScheduledExecutor().shutdown();	
	}
	
	private void selfShutdown(){
		this.executor.shutdown();
	}
	
	private void executeProducersAndConsumers(List<Runnable> jobs){
		for(Runnable job:jobs)
			executor.execute(job);
	}
}
