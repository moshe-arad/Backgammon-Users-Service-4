package org.moshe.arad.local.snapshot;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.ExistingUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.LoggedInEvent;
import org.moshe.arad.kafka.events.LogoutUserEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.kafka.producers.commands.ISimpleCommandProducer;
import org.moshe.arad.kafka.producers.commands.PullEventsWithoutSavingCommandsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Repository
public class SnapshotAPI implements ApplicationContextAware {
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	@Autowired
	private StringRedisTemplate stringRedisTemplate;
	
	private ApplicationContext context;
	
	private Map<UUID, LinkedList<BackgammonEvent>> eventsfromMongo = new HashMap<>();
	
	private Map<UUID, Thread> lockers = new HashMap<>(10000);
	
	private Logger logger = LoggerFactory.getLogger(SnapshotAPI.class);
	
	private Set<Object> updateSnapshotLocker = new HashSet<>(100000);
	
	private ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
	
	private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	
	public static final String LAST_UPDATED = "lastUpdateSnapshotDate";
	
	public static final String USERS = "Users";
	
	public boolean isLastUpdateDateExists(){
		try{
			readWriteLock.readLock().lock();			
			return redisTemplate.hasKey(LAST_UPDATED);
		}
		finally{
			readWriteLock.readLock().unlock();
		}		
	}
	
	public Date getLastUpdateDate(){
		if(isLastUpdateDateExists()){
			try{
				readWriteLock.readLock().lock();			
				return new Date(Long.parseLong(stringRedisTemplate.opsForValue().get(LAST_UPDATED)));
			}
			finally {
				readWriteLock.readLock().unlock();
			}
		}
		else return null;
	}

	public void saveLatestSnapshotDate(Date date){
		readWriteLock.writeLock().lock();
		redisTemplate.opsForValue().set(LAST_UPDATED, Long.toString(date.getTime()));
		readWriteLock.writeLock().unlock();
	}
	
	public Map<String,Map<Object, Object>> readLatestSnapshot(){
		if(!isLastUpdateDateExists()) return null;
		else{
			readWriteLock.readLock().lock();
			Map<Object, Object> users = redisTemplate.opsForHash().entries(USERS);
			readWriteLock.readLock().unlock();
			
			Map<String,Map<Object, Object>> result = new HashMap<String, Map<Object, Object>>(10000);
			if(users != null) result.put(USERS, users);
			return result;
		}
	}	
	
	public Map<String,Map<Object, Object>> doEventsFoldingAndGetInstanceWithoutSaving(){		
		long startTime = System.nanoTime();
		
		logger.info("Preparing command producer...");
		PullEventsWithoutSavingCommandsProducer pullEventsWithoutSavingCommandsProducer = context.getBean(PullEventsWithoutSavingCommandsProducer.class);
		UUID uuid = initSingleProducer(pullEventsWithoutSavingCommandsProducer, KafkaUtils.PULL_EVENTS_WITHOUT_SAVING_COMMAND_TOPIC);
		threadPoolExecutor.submit(pullEventsWithoutSavingCommandsProducer);
		logger.info("command submitted...");
		
		Thread current = Thread.currentThread();
		
		synchronized (current) {
			try {				
				lockers.put(uuid, current);
				current.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		long endTime = System.nanoTime();

		long duration = (endTime - startTime);
		logger.info("***************************************");
		logger.info("****&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&***************");
		logger.info("*******  duration = "+ duration+" *********");
		logger.info("***************************************");
		
		
		
		long startTime1 = System.nanoTime();
		Map<String,Map<Object, Object>> result = getInstanceFromEventsFold(eventsfromMongo.get(uuid));
		long endTime1 = System.nanoTime();
		
		long duration1 = (endTime1 - startTime1);
		logger.info("***************************************");
		logger.info("*******&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&**********");
		logger.info("*******  duration = "+ duration1+" *********");
		logger.info("***************************************");
		
		return result;
	}
	
	private UUID initSingleProducer(ISimpleCommandProducer producer, String topic) {
		producer.setPeriodic(false);
		producer.setTopic(topic);
		producer.setUuid(UUID.randomUUID());
		return producer.getUuid();
	}
	
	public void updateLatestSnapshot(Map<String,Map<Object, Object>> snapshot){
		
		readWriteLock.writeLock().lock();
		
		redisTemplate.expire(LAST_UPDATED, 1, TimeUnit.NANOSECONDS);
		redisTemplate.expire(USERS, 1, TimeUnit.NANOSECONDS);
		
		while(redisTemplate.hasKey(USERS) || redisTemplate.hasKey(LAST_UPDATED)){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		
		saveDataFromSnapshot(snapshot, USERS);
		readWriteLock.writeLock().unlock();				
	}

	private void saveDataFromSnapshot(Map<String,Map<Object, Object>> snapshot, String key) {
		redisTemplate.opsForHash().putAll(key, snapshot.get(key));
	}
	
	/**
	 * calculate instance after events fold, will return this instance without saving it into Redis.
	 * @return 
	 */
	public Map<String,Map<Object, Object>> getInstanceFromEventsFold(LinkedList<BackgammonEvent> fromMongoEventsStoreEventList){
		boolean isLatestSnapshotExists = this.readLatestSnapshot() == null ? false : true;
		Map<String,Map<Object, Object>> currentSnapshot;
		
		if(isLatestSnapshotExists) currentSnapshot = this.readLatestSnapshot();
		else {
			currentSnapshot = new HashMap<>(10000);
			currentSnapshot.put(USERS, new HashMap<>());
		}

		ListIterator<BackgammonEvent> it = fromMongoEventsStoreEventList.listIterator();
		
		logger.info("Starting to fold events into current state...");
		
		while(it.hasNext()){			
			BackgammonEvent eventToFold = it.next();
			logger.info("Event to fold = " + eventToFold);
			if(eventToFold.getClazz().equals("NewUserCreatedEvent")){
				NewUserCreatedEvent newUserCreatedEvent = (NewUserCreatedEvent) eventToFold;
				BackgammonUser backgammonUser  = newUserCreatedEvent.getBackgammonUser();
				
				ObjectMapper objectMapper = new ObjectMapper();
				String backgammonUserJson = null;
				try {
					backgammonUserJson = objectMapper.writeValueAsString(backgammonUser);
				} catch (JsonProcessingException e) {
					logger.error("Failed convert backgammon user to json...");
					logger.error(e.getMessage());
					e.printStackTrace();
				}				
				currentSnapshot.get(USERS).put(backgammonUser.getUserName(), backgammonUserJson);
			}
			else if(eventToFold.getClazz().equals("NewUserJoinedLobbyEvent")){
				NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = (NewUserJoinedLobbyEvent) eventToFold;
				BackgammonUser backgammonUser = newUserJoinedLobbyEvent.getBackgammonUser();
				
				ObjectMapper objectMapper = new ObjectMapper();
				String backgammonUserJson = null;
				try {
					backgammonUserJson = objectMapper.writeValueAsString(backgammonUser);
				} catch (JsonProcessingException e) {
					logger.error("Failed convert backgammon user to json...");
					logger.error(e.getMessage());
					e.printStackTrace();
				}
				
				currentSnapshot.get(USERS).put(backgammonUser.getUserName(), backgammonUserJson);
			}
			else if(eventToFold.getClazz().equals("LoggedInEvent")){
				LoggedInEvent loggedInEvent = (LoggedInEvent) eventToFold;
				BackgammonUser backgammonUser = loggedInEvent.getBackgammonUser();
				
				ObjectMapper objectMapper = new ObjectMapper();
				String backgammonUserJson = null;
				try {
					backgammonUserJson = objectMapper.writeValueAsString(backgammonUser);
				} catch (JsonProcessingException e) {
					logger.error("Failed convert backgammon user to json...");
					logger.error(e.getMessage());
					e.printStackTrace();
				}
	
				currentSnapshot.get(USERS).put(backgammonUser.getUserName(), backgammonUserJson);
			}
			else if(eventToFold.getClazz().equals("ExistingUserJoinedLobbyEvent")){
				ExistingUserJoinedLobbyEvent existingUserJoinedLobbyEvent = (ExistingUserJoinedLobbyEvent) eventToFold;
				BackgammonUser backgammonUser = existingUserJoinedLobbyEvent.getBackgammonUser();
				
				ObjectMapper objectMapper = new ObjectMapper();
				String backgammonUserJson = null;
				try {
					backgammonUserJson = objectMapper.writeValueAsString(backgammonUser);
				} catch (JsonProcessingException e) {
					logger.error("Failed convert backgammon user to json...");
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			
				currentSnapshot.get(USERS).put(backgammonUser.getUserName(), backgammonUserJson);
			}
			else if(eventToFold.getClazz().equals("LoggedOutEvent")){
				LogoutUserEvent logoutUserEvent = (LogoutUserEvent) eventToFold;
				BackgammonUser backgammonUser = logoutUserEvent.getBackgammonUser();
				
				ObjectMapper objectMapper = new ObjectMapper();
				String backgammonUserJson = null;
				try {
					backgammonUserJson = objectMapper.writeValueAsString(backgammonUser);
				} catch (JsonProcessingException e) {
					logger.error("Failed convert backgammon user to json...");
					logger.error(e.getMessage());
					e.printStackTrace();
				}
		
				currentSnapshot.get(USERS).put(backgammonUser.getUserName(), backgammonUserJson);
			}
			
			logger.info("Event to folded successfuly = " + eventToFold);
			//TODO write code about logging out and in game code
		}
		
		logger.info("Events folding into current state completed...");
		
		fromMongoEventsStoreEventList.clear();
		return currentSnapshot;
	}
	
	public Set<Object> getUpdateSnapshotLocker() {
		return updateSnapshotLocker;
	}

	public void setUpdateSnapshotLocker(Set<Object> updateSnapshotLocker) {
		this.updateSnapshotLocker = updateSnapshotLocker;
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}

	public Map<UUID, Thread> getLockers() {
		return lockers;
	}

	public void setLockers(Map<UUID, Thread> lockers) {
		this.lockers = lockers;
	}

	public Map<UUID, LinkedList<BackgammonEvent>> getEventsfromMongo() {
		return eventsfromMongo;
	}

	public void setEventsfromMongo(Map<UUID, LinkedList<BackgammonEvent>> eventsfromMongo) {
		this.eventsfromMongo = eventsfromMongo;
	}
}
