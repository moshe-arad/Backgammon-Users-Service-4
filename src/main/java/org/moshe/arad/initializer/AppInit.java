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
import org.moshe.arad.kafka.consumers.commands.LogInUserCommandsConsumer;
import org.moshe.arad.kafka.consumers.commands.LogOutUserCommandsConsumer;
import org.moshe.arad.kafka.consumers.config.CreateNewUserCommandConfig;
import org.moshe.arad.kafka.consumers.config.FromMongoWithSavingEventsConfig;
import org.moshe.arad.kafka.consumers.config.FromMongoWithoutSavingEventsConfig;
import org.moshe.arad.kafka.consumers.config.LogInUserCommandConfig;
import org.moshe.arad.kafka.consumers.config.LogOutUserCommandConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutOpenByLeftBeforeGameStartedEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutOpenByLeftEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutOpenByLeftFirstEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutOpenByLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutSecondLeftEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutSecondLeftFirstEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutSecondLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.NewGameRoomOpenedEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenByLeftBeforeGameStartedEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenByLeftEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenByLeftFirstEventConfig;
import org.moshe.arad.kafka.consumers.config.SecondLeftFirstEventConfig;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.consumers.config.UserAddedAsSecondPlayerEventConfig;
import org.moshe.arad.kafka.consumers.config.UserAddedAsWatcherEventConfig;
import org.moshe.arad.kafka.consumers.config.WatcherLeftEventConfig;
import org.moshe.arad.kafka.consumers.config.WatcherLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutUserLeftLobbyEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutWatcherLeftEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutWatcherLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.events.FromMongoWithSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.FromMongoWithoutSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutOpenByLeftBeforeGameStartedEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutOpenByLeftEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutOpenByLeftFirstEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutOpenByLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutSecondLeftEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutSecondLeftFirstEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutSecondLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.NewGameRoomOpenedEventConsumer;
import org.moshe.arad.kafka.consumers.events.OpenByLeftBeforeGameStartedEventConsumer;
import org.moshe.arad.kafka.consumers.events.OpenByLeftEventConsumer;
import org.moshe.arad.kafka.consumers.events.OpenByLeftFirstEventConsumer;
import org.moshe.arad.kafka.consumers.events.SecondLeftFirstEventConsumer;
import org.moshe.arad.kafka.consumers.events.UserAddedAsSecondPlayerEventConsumer;
import org.moshe.arad.kafka.consumers.events.UserAddedAsWatcherEventConsumer;
import org.moshe.arad.kafka.consumers.events.WatcherLeftEventConsumer;
import org.moshe.arad.kafka.consumers.events.WatcherLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutUserLeftLobbyEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutWatcherLeftEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutWatcherLeftLastEventConsumer;
import org.moshe.arad.kafka.events.LogInUserAckEvent;
import org.moshe.arad.kafka.events.LoggedInEvent;
import org.moshe.arad.kafka.events.LoggedOutEvent;
import org.moshe.arad.kafka.events.NewUserCreatedAckEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.UserPermissionsUpdatedEvent;
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
	
	private LogInUserCommandsConsumer logInUserCommandsConsumer;
	
	@Autowired
	private LogInUserCommandConfig logInUserCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<LogInUserAckEvent> logInUserAckEventsProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedInEvent> loggedInEventsProducer;
	
	private NewGameRoomOpenedEventConsumer newGameRoomOpenedEventConsumer;
	
	@Autowired
	private NewGameRoomOpenedEventConfig newGameRoomOpenedEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedEventProducer;
	
	private UserAddedAsWatcherEventConsumer userAddedAsWatcherEventConsumer;
	
	@Autowired
	private UserAddedAsWatcherEventConfig userAddedAsWatcherEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterAddWatcherEventProducer;
	
	private UserAddedAsSecondPlayerEventConsumer userAddedAsSecondPlayerEventConsumer;
	
	@Autowired
	private UserAddedAsSecondPlayerEventConfig userAddedAsSecondPlayerEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterAddSecondPlayerEventProducer;
	
	private LogOutUserCommandsConsumer logOutUserCommandConsumer;
	
	@Autowired
	private LogOutUserCommandConfig logOutUserCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutEvent> loggedOutEventProducer;
	
	private LoggedOutUserLeftLobbyEventConsumer loggedOutUserLeftLobbyEventConsumer;
	
	@Autowired
	private LoggedOutUserLeftLobbyEventConfig loggedOutUserLeftLobbyEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterLeftLobbyProducer;
	
	private LoggedOutOpenByLeftBeforeGameStartedEventConsumer loggedOutOpenByLeftBeforeGameStartedEventConsumer;
	
	@Autowired
	private LoggedOutOpenByLeftBeforeGameStartedEventConfig loggedOutOpenByLeftBeforeGameStartedEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutOpenByLeftBeforeGameStartedProducer;
	
	private LoggedOutOpenByLeftEventConsumer loggedOutOpenByLeftEventConsumer;
	
	@Autowired
	private LoggedOutOpenByLeftEventConfig loggedOutOpenByLeftEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutOpenByLeftProducer;
	
	private LoggedOutWatcherLeftLastEventConsumer loggedOutWatcherLeftLastEventConsumer;
	
	@Autowired
	private LoggedOutWatcherLeftLastEventConfig loggedOutWatcherLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutWatcherLeftLastProducer;
	
	private LoggedOutWatcherLeftEventConsumer loggedOutWatcherLeftEventConsumer;
	
	@Autowired
	private LoggedOutWatcherLeftEventConfig loggedOutWatcherLeftEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutWatcherLeftProducer;
	
	private LoggedOutOpenByLeftFirstEventConsumer loggedOutOpenByLeftFirstEventConsumer;
	
	@Autowired
	private LoggedOutOpenByLeftFirstEventConfig loggedOutOpenByLeftFirstEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutOpenByLeftFirstProducer;
	
	private LoggedOutSecondLeftFirstEventConsumer loggedOutSecondLeftFirstEventConsumer;
	
	@Autowired
	private LoggedOutSecondLeftFirstEventConfig loggedOutSecondLeftFirstEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutSecondLeftFirstProducer;
	
	private LoggedOutSecondLeftEventConsumer loggedOutSecondLeftEventConsumer;
	
	@Autowired
	private LoggedOutSecondLeftEventConfig loggedOutSecondLeftEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutSecondLeftProducer;
	
	private LoggedOutOpenByLeftLastEventConsumer loggedOutOpenByLeftLastEventConsumer;
	
	@Autowired
	private LoggedOutOpenByLeftLastEventConfig loggedOutOpenByLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutOpenByLeftLastProducer;
	
	private LoggedOutSecondLeftLastEventConsumer loggedOutSecondLeftLastEventConsumer;
	
	@Autowired
	private LoggedOutSecondLeftLastEventConfig loggedOutSecondLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterloggedOutSecondLeftLastProducer;
	
	private OpenByLeftBeforeGameStartedEventConsumer openByLeftBeforeGameStartedEventConsumer;
	
	@Autowired
	private OpenByLeftBeforeGameStartedEventConfig openByLeftBeforeGameStartedEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterOpenByLeftBeforeGameStartedEventProducer;

	private OpenByLeftEventConsumer openByLeftEventConsumer;
	
	@Autowired
	private OpenByLeftEventConfig openByLeftEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterOpenByLeftEventProducer;

	private WatcherLeftLastEventConsumer watcherLeftLastEventConsumer;
	
	@Autowired
	private WatcherLeftLastEventConfig watcherLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterWatcherLeftLastEventProducer;

	private WatcherLeftEventConsumer watcherLeftEventConsumer;
	
	@Autowired
	private WatcherLeftEventConfig watcherLeftEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterWatcherLeftEventProducer;

	private OpenByLeftFirstEventConsumer openByLeftFirstEventConsumer;
	
	@Autowired
	private OpenByLeftFirstEventConfig openByLeftFirstEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterOpenByLeftFirstEventProducer;

	private SecondLeftFirstEventConsumer secondLeftFirstEventConsumer;
	
	@Autowired
	private SecondLeftFirstEventConfig secondLeftFirstEventConfig;
	
	@Autowired
	private SimpleEventsProducer<UserPermissionsUpdatedEvent> userPermissionsUpdatedAfterSecondLeftFirstEventProducer;

	private ApplicationContext context;
	
	private ExecutorService executor = Executors.newFixedThreadPool(4);
	
	private ConsumerToProducerQueue toLobbyServiceQueue;
	
	private ConsumerToProducerQueue toFrontServiceQueue;
	
	private ConsumerToProducerQueue toViewServiceLogInUserCommandQueue;
	
	private ConsumerToProducerQueue toFrontServiceLogInUserCommandQueue;
	
	private ConsumerToProducerQueue userPermissionsUpdatedQueue;
	
	private ConsumerToProducerQueue userAddedAsWatcherQueue;
	
	private ConsumerToProducerQueue userAddedAsSecondPlayerQueue;
	
	private ConsumerToProducerQueue logOutQueue;
	
	private ConsumerToProducerQueue loggedOutUserLeftlobbyQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftBeforeGameStartedQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftQueue;
	
	private ConsumerToProducerQueue loggedOutWatcherLeftLastQueue;
	
	private ConsumerToProducerQueue loggedOutWatcherLeftQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftFirstQueue;
	
	private ConsumerToProducerQueue loggedOutSecondLeftFirstQueue;
	
	private ConsumerToProducerQueue loggedOutSecondLeftQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftLastQueue;
	
	private ConsumerToProducerQueue loggedOutSecondLeftLastQueue;
	
	private ConsumerToProducerQueue watcherLeftLastQueue;
	
	private ConsumerToProducerQueue watcherLeftQueue;
	
	private ConsumerToProducerQueue openByLeftFirstQueue;
	
	private ConsumerToProducerQueue secondLeftFirstQueue;
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	private ConsumerToProducerQueue openByLeftBeforeGameStartedQueue;
	
	private ConsumerToProducerQueue openByLeftQueue;
	
	public static final int NUM_CONSUMERS = 3;
	
	public AppInit() {
		
	}
	
	@Override
	public void initKafkaCommandsConsumers() {	
		toLobbyServiceQueue = context.getBean(ConsumerToProducerQueue.class);
		toFrontServiceQueue = context.getBean(ConsumerToProducerQueue.class);
		toViewServiceLogInUserCommandQueue = context.getBean(ConsumerToProducerQueue.class);
		toFrontServiceLogInUserCommandQueue = context.getBean(ConsumerToProducerQueue.class);		
		logOutQueue = context.getBean(ConsumerToProducerQueue.class);
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			createNewUserCommandConsumer = context.getBean(CreateNewUserCommandsConsumer.class);
			logInUserCommandsConsumer = context.getBean(LogInUserCommandsConsumer.class);
			logOutUserCommandConsumer = context.getBean(LogOutUserCommandsConsumer.class);
			
			logger.info("Initializing create new user command consumer...");			
			createNewUserCommandConsumer.setToLobbyServiceQueue(toLobbyServiceQueue);
			createNewUserCommandConsumer.setToFrontServiceQueue(toFrontServiceQueue);			
			initSingleConsumer(createNewUserCommandConsumer, KafkaUtils.CREATE_NEW_USER_COMMAND_TOPIC, createNewUserCommandConfig);			
			
			logInUserCommandsConsumer.setToViewServiceQueue(toViewServiceLogInUserCommandQueue);
			logInUserCommandsConsumer.setToFrontServiceQueue(toFrontServiceLogInUserCommandQueue);
			initSingleConsumer(logInUserCommandsConsumer, KafkaUtils.LOG_IN_USER_COMMAND_TOPIC, logInUserCommandConfig);
			
			logger.info("Initialize create new user command consumer, completed...");
			
			logOutUserCommandConsumer.setConsumerToProducerQueue(logOutQueue);
			initSingleConsumer(logOutUserCommandConsumer, KafkaUtils.LOG_OUT_USER_COMMAND_TOPIC, logOutUserCommandConfig);
			
			executeRunnablesProducersAndConsumers(Arrays.asList(createNewUserCommandConsumer, 
					logInUserCommandsConsumer,
					logOutUserCommandConsumer));
		}
	}

	@Override
	public void initKafkaEventsConsumers() {
		
		userPermissionsUpdatedQueue = context.getBean(ConsumerToProducerQueue.class);
		userAddedAsWatcherQueue = context.getBean(ConsumerToProducerQueue.class);
		userAddedAsSecondPlayerQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutUserLeftlobbyQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftBeforeGameStartedQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutWatcherLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutWatcherLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutSecondLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutSecondLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutSecondLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		openByLeftBeforeGameStartedQueue = context.getBean(ConsumerToProducerQueue.class);
		openByLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		watcherLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		watcherLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		openByLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		secondLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			fromMongoWithSavingEventsConsumer = context.getBean(FromMongoWithSavingEventsConsumer.class);
			fromMongoWithoutSavingEventsConsumer = context.getBean(FromMongoWithoutSavingEventsConsumer.class);
			newGameRoomOpenedEventConsumer = context.getBean(NewGameRoomOpenedEventConsumer.class);
			userAddedAsWatcherEventConsumer = context.getBean(UserAddedAsWatcherEventConsumer.class);
			userAddedAsSecondPlayerEventConsumer = context.getBean(UserAddedAsSecondPlayerEventConsumer.class);
			loggedOutUserLeftLobbyEventConsumer = context.getBean(LoggedOutUserLeftLobbyEventConsumer.class);
			loggedOutOpenByLeftBeforeGameStartedEventConsumer = context.getBean(LoggedOutOpenByLeftBeforeGameStartedEventConsumer.class);
			loggedOutOpenByLeftEventConsumer = context.getBean(LoggedOutOpenByLeftEventConsumer.class);
			loggedOutWatcherLeftLastEventConsumer = context.getBean(LoggedOutWatcherLeftLastEventConsumer.class);
			loggedOutWatcherLeftEventConsumer = context.getBean(LoggedOutWatcherLeftEventConsumer.class);
			loggedOutOpenByLeftFirstEventConsumer = context.getBean(LoggedOutOpenByLeftFirstEventConsumer.class);
			loggedOutSecondLeftFirstEventConsumer = context.getBean(LoggedOutSecondLeftFirstEventConsumer.class);
			loggedOutSecondLeftEventConsumer = context.getBean(LoggedOutSecondLeftEventConsumer.class);
			loggedOutOpenByLeftLastEventConsumer = context.getBean(LoggedOutOpenByLeftLastEventConsumer.class);
			loggedOutSecondLeftLastEventConsumer = context.getBean(LoggedOutSecondLeftLastEventConsumer.class);
			openByLeftBeforeGameStartedEventConsumer = context.getBean(OpenByLeftBeforeGameStartedEventConsumer.class);
			openByLeftEventConsumer = context.getBean(OpenByLeftEventConsumer.class);
			watcherLeftLastEventConsumer = context.getBean(WatcherLeftLastEventConsumer.class);
			watcherLeftEventConsumer = context.getBean(WatcherLeftEventConsumer.class);
			openByLeftFirstEventConsumer = context.getBean(OpenByLeftFirstEventConsumer.class);
			secondLeftFirstEventConsumer = context.getBean(SecondLeftFirstEventConsumer.class);
			
			logger.info("Initializing from mongo events store event consumer...");
			initSingleConsumer(fromMongoWithSavingEventsConsumer, KafkaUtils.FROM_MONGO_EVENTS_WITH_SAVING_TOPIC, fromMongoWithSavingEventsConfig);
						
			
			initSingleConsumer(fromMongoWithoutSavingEventsConsumer, KafkaUtils.FROM_MONGO_EVENTS_WITHOUT_SAVING_TOPIC, fromMongoWithoutSavingEventsConfig);
			logger.info("Initialize from mongo events store event consumer, completed...");
			
			newGameRoomOpenedEventConsumer.setConsumerToProducerQueue(userPermissionsUpdatedQueue);
			initSingleConsumer(newGameRoomOpenedEventConsumer, KafkaUtils.NEW_GAME_ROOM_OPENED_EVENT_TOPIC, newGameRoomOpenedEventConfig);
			
			userAddedAsWatcherEventConsumer.setConsumerToProducerQueue(userAddedAsWatcherQueue);
			initSingleConsumer(userAddedAsWatcherEventConsumer, KafkaUtils.USER_ADDED_AS_WATCHER_EVENT_TOPIC, userAddedAsWatcherEventConfig);
			
			userAddedAsSecondPlayerEventConsumer.setConsumerToProducerQueue(userAddedAsSecondPlayerQueue);
			initSingleConsumer(userAddedAsSecondPlayerEventConsumer, KafkaUtils.USER_ADDED_AS_SECOND_PLAYER_EVENT_TOPIC, userAddedAsSecondPlayerEventConfig);
			
			loggedOutUserLeftLobbyEventConsumer.setConsumerToProducerQueue(loggedOutUserLeftlobbyQueue);
			initSingleConsumer(loggedOutUserLeftLobbyEventConsumer, KafkaUtils.LOGGED_OUT_USER_LEFT_LOBBY_EVENT_TOPIC, loggedOutUserLeftLobbyEventConfig);
			
			loggedOutOpenByLeftBeforeGameStartedEventConsumer.setConsumerToProducerQueue(loggedOutOpenByLeftBeforeGameStartedQueue);
			initSingleConsumer(loggedOutOpenByLeftBeforeGameStartedEventConsumer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, loggedOutOpenByLeftBeforeGameStartedEventConfig);
			
			loggedOutOpenByLeftEventConsumer.setConsumerToProducerQueue(loggedOutOpenByLeftQueue);
			initSingleConsumer(loggedOutOpenByLeftEventConsumer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_EVENT_TOPIC, loggedOutOpenByLeftEventConfig);
			
			loggedOutWatcherLeftLastEventConsumer.setConsumerToProducerQueue(loggedOutWatcherLeftLastQueue);
			initSingleConsumer(loggedOutWatcherLeftLastEventConsumer, KafkaUtils.LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_TOPIC, loggedOutWatcherLeftLastEventConfig);
			
			loggedOutWatcherLeftEventConsumer.setConsumerToProducerQueue(loggedOutWatcherLeftQueue);
			initSingleConsumer(loggedOutWatcherLeftEventConsumer, KafkaUtils.LOGGED_OUT_WATCHER_LEFT_EVENT_TOPIC, loggedOutWatcherLeftEventConfig);
			
			loggedOutOpenByLeftFirstEventConsumer.setConsumerToProducerQueue(loggedOutOpenByLeftFirstQueue);
			initSingleConsumer(loggedOutOpenByLeftFirstEventConsumer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_FIRST_EVENT_TOPIC, loggedOutOpenByLeftFirstEventConfig);
			
			loggedOutSecondLeftFirstEventConsumer.setConsumerToProducerQueue(loggedOutSecondLeftFirstQueue);
			initSingleConsumer(loggedOutSecondLeftFirstEventConsumer, KafkaUtils.LOGGED_OUT_SECOND_LEFT_FIRST_EVENT_TOPIC, loggedOutSecondLeftFirstEventConfig);
			
			loggedOutSecondLeftEventConsumer.setConsumerToProducerQueue(loggedOutSecondLeftQueue);
			initSingleConsumer(loggedOutSecondLeftEventConsumer, KafkaUtils.LOGGED_OUT_SECOND_LEFT_EVENT_TOPIC, loggedOutSecondLeftEventConfig);
			
			loggedOutOpenByLeftLastEventConsumer.setConsumerToProducerQueue(loggedOutOpenByLeftLastQueue);
			initSingleConsumer(loggedOutOpenByLeftLastEventConsumer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_TOPIC, loggedOutOpenByLeftLastEventConfig);
			
			loggedOutSecondLeftLastEventConsumer.setConsumerToProducerQueue(loggedOutSecondLeftLastQueue);
			initSingleConsumer(loggedOutSecondLeftLastEventConsumer, KafkaUtils.LOGGED_OUT_SECOND_LEFT_LAST_EVENT_TOPIC, loggedOutSecondLeftLastEventConfig);
			
			openByLeftBeforeGameStartedEventConsumer.setConsumerToProducerQueue(openByLeftBeforeGameStartedQueue);
			initSingleConsumer(openByLeftBeforeGameStartedEventConsumer, KafkaUtils.OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, openByLeftBeforeGameStartedEventConfig);
			
			openByLeftEventConsumer.setConsumerToProducerQueue(openByLeftQueue);
			initSingleConsumer(openByLeftEventConsumer, KafkaUtils.OPENBY_LEFT_EVENT_TOPIC, openByLeftEventConfig);
						
			watcherLeftLastEventConsumer.setConsumerToProducerQueue(watcherLeftLastQueue);
			initSingleConsumer(watcherLeftLastEventConsumer, KafkaUtils.WATCHER_LEFT_LAST_EVENT_TOPIC, watcherLeftLastEventConfig);
			
			watcherLeftEventConsumer.setConsumerToProducerQueue(watcherLeftQueue);
			initSingleConsumer(watcherLeftEventConsumer, KafkaUtils.WATCHER_LEFT_EVENT_TOPIC, watcherLeftEventConfig);
			
			openByLeftFirstEventConsumer.setConsumerToProducerQueue(openByLeftFirstQueue);
			initSingleConsumer(openByLeftFirstEventConsumer, KafkaUtils.OPENBY_LEFT_FIRST_EVENT_TOPIC, openByLeftFirstEventConfig);
			
			secondLeftFirstEventConsumer.setConsumerToProducerQueue(secondLeftFirstQueue);
			initSingleConsumer(secondLeftFirstEventConsumer, KafkaUtils.SECOND_LEFT_FIRST_EVENT_TOPIC, secondLeftFirstEventConfig);
			
			executeRunnablesProducersAndConsumers(Arrays.asList(fromMongoWithSavingEventsConsumer, 
					fromMongoWithoutSavingEventsConsumer,
					newGameRoomOpenedEventConsumer,
					userAddedAsWatcherEventConsumer,
					userAddedAsSecondPlayerEventConsumer,
					loggedOutUserLeftLobbyEventConsumer,
					loggedOutOpenByLeftBeforeGameStartedEventConsumer,
					loggedOutOpenByLeftEventConsumer,
					loggedOutWatcherLeftLastEventConsumer,
					loggedOutWatcherLeftEventConsumer,
					loggedOutOpenByLeftFirstEventConsumer,
					loggedOutSecondLeftFirstEventConsumer,
					loggedOutSecondLeftEventConsumer,
					loggedOutOpenByLeftLastEventConsumer,
					loggedOutSecondLeftLastEventConsumer,
					openByLeftBeforeGameStartedEventConsumer,
					openByLeftEventConsumer,
					watcherLeftLastEventConsumer,
					watcherLeftEventConsumer,
					openByLeftFirstEventConsumer,
					secondLeftFirstEventConsumer));
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
		
		initSingleProducer(logInUserAckEventsProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.LOG_IN_USER_ACK_EVENT_TOPIC, toFrontServiceLogInUserCommandQueue);
		initSingleProducer(loggedInEventsProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.LOGGED_IN_EVENT_TOPIC, toViewServiceLogInUserCommandQueue);
				
		logger.info("Initialize new user created events producer, completed...");
		
		initSingleProducer(userPermissionsUpdatedEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_EVENT_TOPIC, userPermissionsUpdatedQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterAddWatcherEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_USER_ADDED_WATCHER_EVENT_TOPIC, userAddedAsWatcherQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterAddSecondPlayerEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_USER_ADDED_SECOND_PLAYER_EVENT_TOPIC, userAddedAsSecondPlayerQueue);
		
		initSingleProducer(loggedOutEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.LOGGED_OUT_EVENT_TOPIC, logOutQueue);
	
		initSingleProducer(userPermissionsUpdatedAfterLeftLobbyProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_USER_LEFT_LOBBY_EVENT_TOPIC, loggedOutUserLeftlobbyQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutOpenByLeftBeforeGameStartedProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, loggedOutOpenByLeftBeforeGameStartedQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutOpenByLeftProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_OPENBY_LEFT_EVENT_TOPIC, loggedOutOpenByLeftQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutWatcherLeftLastProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_TOPIC, loggedOutWatcherLeftLastQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutWatcherLeftProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_WATCHER_LEFT_EVENT_TOPIC, loggedOutWatcherLeftQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutOpenByLeftFirstProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_OPENBY_LEFT_FIRST_EVENT_TOPIC, loggedOutOpenByLeftFirstQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutSecondLeftFirstProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_SECOND_LEFT_FIRST_EVENT_TOPIC, loggedOutSecondLeftFirstQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutSecondLeftProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_SECOND_LEFT_EVENT_TOPIC, loggedOutSecondLeftQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutOpenByLeftLastProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_TOPIC, loggedOutOpenByLeftLastQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterloggedOutSecondLeftLastProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_LOGGED_OUT_SECOND_LEFT_LAST_EVENT_TOPIC, loggedOutSecondLeftLastQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterOpenByLeftBeforeGameStartedEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, openByLeftBeforeGameStartedQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterOpenByLeftEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_OPENBY_LEFT_EVENT_TOPIC, openByLeftQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterWatcherLeftLastEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_WATCHER_LEFT_LAST_EVENT_TOPIC, watcherLeftLastQueue);

		initSingleProducer(userPermissionsUpdatedAfterWatcherLeftEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_WATCHER_LEFT_EVENT_TOPIC, watcherLeftQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterOpenByLeftFirstEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_OPENBY_LEFT_FIRST_EVENT_TOPIC, openByLeftFirstQueue);
		
		initSingleProducer(userPermissionsUpdatedAfterSecondLeftFirstEventProducer, 10, 0, TimeUnit.MILLISECONDS, KafkaUtils.USER_PERMISSIONS_UPDATED_SECOND_LEFT_FIRST_EVENT_TOPIC, secondLeftFirstQueue);
		
		executeRunnablesProducersAndConsumers(Arrays.asList(newUserCreatedEventsProducer, newUserCreatedAckEventsProducer,
				logInUserAckEventsProducer,loggedInEventsProducer,
				userPermissionsUpdatedEventProducer,
				userPermissionsUpdatedAfterAddWatcherEventProducer,
				userPermissionsUpdatedAfterAddSecondPlayerEventProducer,
				loggedOutEventProducer,
				userPermissionsUpdatedAfterLeftLobbyProducer,
				userPermissionsUpdatedAfterloggedOutOpenByLeftBeforeGameStartedProducer,
				userPermissionsUpdatedAfterloggedOutOpenByLeftProducer,
				userPermissionsUpdatedAfterloggedOutWatcherLeftLastProducer,
				userPermissionsUpdatedAfterloggedOutWatcherLeftProducer,
				userPermissionsUpdatedAfterloggedOutOpenByLeftFirstProducer,
				userPermissionsUpdatedAfterloggedOutSecondLeftFirstProducer,
				userPermissionsUpdatedAfterloggedOutSecondLeftProducer,
				userPermissionsUpdatedAfterloggedOutOpenByLeftLastProducer,
				userPermissionsUpdatedAfterloggedOutSecondLeftLastProducer,
				userPermissionsUpdatedAfterOpenByLeftBeforeGameStartedEventProducer,
				userPermissionsUpdatedAfterOpenByLeftEventProducer,
				userPermissionsUpdatedAfterWatcherLeftLastEventProducer,
				userPermissionsUpdatedAfterWatcherLeftEventProducer,
				userPermissionsUpdatedAfterOpenByLeftFirstEventProducer,
				userPermissionsUpdatedAfterSecondLeftFirstEventProducer));
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
