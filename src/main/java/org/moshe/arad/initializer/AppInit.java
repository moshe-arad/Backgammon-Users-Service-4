package org.moshe.arad.initializer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.ISimpleConsumer;
import org.moshe.arad.kafka.consumers.commands.CreateNewUserCommandsConsumer;
import org.moshe.arad.kafka.consumers.config.CreateNewUserCommandConfig;
import org.moshe.arad.kafka.consumers.config.FromMongoWithSavingEventsConfig;
import org.moshe.arad.kafka.consumers.config.FromMongoWithoutSavingEventsConfig;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.consumers.events.FromMongoWithSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.FromMongoWithoutSavingEventsConsumer;
import org.moshe.arad.kafka.events.NewUserCreatedAckEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.producers.ISimpleProducer;
import org.moshe.arad.kafka.producers.commands.ISimpleCommandProducer;
import org.moshe.arad.kafka.producers.commands.PullEventsWithSavingCommandsProducer;
import org.moshe.arad.kafka.producers.events.ISimpleEventProducer;
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
	
	private CreateNewUserCommandsConsumer createNewUserCommandConsumer;
	
	@Autowired
	private CreateNewUserCommandConfig createNewUserCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<NewUserCreatedEvent> newUserCreatedEventsProducer;
	
	@Autowired
	private PullEventsWithSavingCommandsProducer pullEventsWithSavingCommandsProducer;
	
	private FromMongoWithSavingEventsConsumer fromMongoWithSavingEventsConsumer;
	
	private FromMongoWithoutSavingEventsConsumer fromMongoWithoutSavingEventsConsumer;

	@Autowired
	private FromMongoWithSavingEventsConfig fromMongoWithSavingEventsConfig;
	
	@Autowired
	private FromMongoWithoutSavingEventsConfig fromMongoWithoutSavingEventsConfig;
	
	@Autowired
	private SimpleEventsProducer<NewUserCreatedAckEvent> newUserCreatedAckEventsProducer;
	
	private ApplicationContext context;
	
	private ExecutorService executor = Executors.newFixedThreadPool(4);
	
	private ConsumerToProducerQueue toLobbyServiceQueue;
	
	private ConsumerToProducerQueue toFrontServiceQueue;
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	public static final int NUM_CONSUMERS = 5;
	
	public AppInit() {
		
	}
	
	@Override
	public void initKafkaCommandsConsumers() {	
		toLobbyServiceQueue = context.getBean(ConsumerToProducerQueue.class);
		toFrontServiceQueue = context.getBean(ConsumerToProducerQueue.class);
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			createNewUserCommandConsumer = context.getBean(CreateNewUserCommandsConsumer.class);
			
			logger.info("Initializing create new user command consumer...");			
			createNewUserCommandConsumer.setToLobbyServiceQueue(toLobbyServiceQueue);
			createNewUserCommandConsumer.setToFrontServiceQueue(toFrontServiceQueue);			
			initSingleConsumer(createNewUserCommandConsumer, KafkaUtils.CREATE_NEW_USER_COMMAND_TOPIC, createNewUserCommandConfig);			
			logger.info("Initialize create new user command consumer, completed...");
			
			executeRunnablesProducersAndConsumers(Arrays.asList(createNewUserCommandConsumer));
		}
	}

	@Override
	public void initKafkaEventsConsumers() {
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			fromMongoWithSavingEventsConsumer = context.getBean(FromMongoWithSavingEventsConsumer.class);
			fromMongoWithoutSavingEventsConsumer = context.getBean(FromMongoWithoutSavingEventsConsumer.class);
			
			logger.info("Initializing from mongo events store event consumer...");
			initSingleConsumer(fromMongoWithSavingEventsConsumer, KafkaUtils.FROM_MONGO_EVENTS_WITH_SAVING_TOPIC, fromMongoWithSavingEventsConfig);
						
			
			initSingleConsumer(fromMongoWithoutSavingEventsConsumer, KafkaUtils.FROM_MONGO_EVENTS_WITHOUT_SAVING_TOPIC, fromMongoWithoutSavingEventsConfig);
			logger.info("Initialize from mongo events store event consumer, completed...");
			
			executeRunnablesProducersAndConsumers(Arrays.asList(fromMongoWithSavingEventsConsumer, fromMongoWithoutSavingEventsConsumer));
		}
	}

	@Override
	public void initKafkaCommandsProducers() {
		logger.info("Initializing pull events commands producer...");
		initSingleProducer(pullEventsWithSavingCommandsProducer, 5, 1, TimeUnit.MINUTES, KafkaUtils.PULL_EVENTS_WITH_SAVING_COMMAND_TOPIC, null);
		logger.info("Initialize pull events commands producer, completed...");
		
		executeRunnablesProducersAndConsumers(Arrays.asList(pullEventsWithSavingCommandsProducer));
	}

	@Override
	public void initKafkaEventsProducers() {		
		logger.info("Initializing new user created events producer...");		
		initSingleProducer(newUserCreatedAckEventsProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.NEW_USER_CREATED_ACK_EVENT_TOPIC, toFrontServiceQueue);		
		initSingleProducer(newUserCreatedEventsProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.NEW_USER_CREATED_EVENT_TOPIC, toLobbyServiceQueue);
		logger.info("Initialize new user created events producer, completed...");
		
		executeRunnablesProducersAndConsumers(Arrays.asList(newUserCreatedEventsProducer, newUserCreatedAckEventsProducer));
	}

	@Override
	public void engineShutdown() {
		logger.info("about to do shutdown.");		
		shutdownSingleConsumer(createNewUserCommandConsumer);
		shutdownSingleConsumer(fromMongoWithSavingEventsConsumer);		
		shutdownSingleProducer(newUserCreatedEventsProducer);
		shutdownSingleProducer(pullEventsWithSavingCommandsProducer);
		selfShutdown();
		logger.info("shutdown compeleted.");
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}
	
	private void initSingleConsumer(ISimpleConsumer consumer, String topic, SimpleConsumerConfig consumerConfig) {
		consumer.setTopic(topic);
		consumer.setSimpleConsumerConfig(consumerConfig);
		consumer.initConsumer();	
	}
	
	private void initSingleProducer(ISimpleCommandProducer producer, int period, int initialDelay, TimeUnit timeUnit, String topic, ConsumerToProducerQueue queue) {
		producer.setPeriodic(true);
		producer.setPeriod(period);
		producer.setInitialDelay(initialDelay);
		producer.setTimeUnit(timeUnit);
		producer.setTopic(topic);	
		producer.setConsumerToProducerQueue(queue);
	}
	
	private void initSingleProducer(ISimpleEventProducer producer, int period, int initialDelay, TimeUnit timeUnit, String topic, ConsumerToProducerQueue queue) {
		producer.setPeriod(period);
		producer.setInitialDelay(initialDelay);
		producer.setTimeUnit(timeUnit);
		producer.setTopic(topic);	
		producer.setConsumerToProducerQueue(queue);
	}
	
	private void shutdownSingleConsumer(ISimpleConsumer consumer) {
		consumer.setRunning(false);
		consumer.getScheduledExecutor().shutdown();
		consumer.closeConsumer();
		
	}
	
	private void shutdownSingleProducer(ISimpleProducer producer) {
		producer.setRunning(false);
		producer.getScheduledExecutor().shutdown();	
	}
	
	private void selfShutdown(){
		this.executor.shutdown();
	}
	
	private void executeRunnablesProducersAndConsumers(List<Runnable> jobs){
		for(Runnable job:jobs)
			executor.execute(job);
	}
	
	private void executeCallablesProducersAndConsumers(List<Callable> jobs){
		for(Callable job:jobs)
			executor.submit(job);
	}
}
