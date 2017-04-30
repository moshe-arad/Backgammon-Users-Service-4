package org.moshe.arad.local.snapshot;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
	public static final String LOBBY = "Lobby";
	public static final String GAME = "Game";
	public static final String LOGGED_OUT = "LoggedOut";
	
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
	
	public Map<String,Set<String>> readLatestSnapshot(){
		if(!isLastUpdateDateExists()) return null;
		else{
			readWriteLock.readLock().lock();
			
			Set<String> usersInLobby = redisTemplate.opsForSet().members(LOBBY);
			Set<String> usersInGame = redisTemplate.opsForSet().members(GAME);
			Set<String> usersLoggedOut = redisTemplate.opsForSet().members(LOGGED_OUT);
			
			readWriteLock.readLock().unlock();
			
			Map<String,Set<String>> result = new HashMap<String, Set<String>>(10000);
			
			result.put(LOBBY, usersInLobby);
			result.put(GAME, usersInGame);
			result.put(LOGGED_OUT, usersLoggedOut);
			return result;
		}
	}	
	
	public Map<String,Set<String>> doEventsFoldingAndGetInstanceWithoutSaving(){		
		logger.info("Preparing command producer...");
		PullEventsWithoutSavingCommandsProducer pullEventsWithoutSavingCommandsProducer = context.getBean(PullEventsWithoutSavingCommandsProducer.class);
		UUID uuid = initSingleProducer(pullEventsWithoutSavingCommandsProducer, KafkaUtils.PULL_EVENTS_WITHOUT_SAVING_COMMAND_TOPIC);
		threadPoolExecutor.submit(pullEventsWithoutSavingCommandsProducer);
		logger.info("command submitted...");
		
		Thread current = Thread.currentThread();
		
		synchronized (current) {
			try {				
				lockers.put(uuid, current);
				current.wait(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return getInstanceFromEventsFold(eventsfromMongo.get(uuid));
	}
	
	private UUID initSingleProducer(ISimpleCommandProducer producer, String topic) {
		producer.setPeriodic(false);
		producer.setTopic(topic);
		producer.setUuid(UUID.randomUUID());
		return producer.getUuid();
	}
	
	public void updateLatestSnapshot(Map<String,Set<String>> snapshot){
		
		readWriteLock.writeLock().lock();
		
		redisTemplate.expire(LOBBY, 1, TimeUnit.NANOSECONDS);
		redisTemplate.expire(GAME, 1, TimeUnit.NANOSECONDS);
		redisTemplate.expire(LAST_UPDATED, 1, TimeUnit.NANOSECONDS);
		
		while(redisTemplate.hasKey(LOBBY) || redisTemplate.hasKey(GAME) || redisTemplate.hasKey(LOGGED_OUT)){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		String[] lobbyUsers = new String[snapshot.get(LOBBY).size()];
		if(lobbyUsers.length > 0) {
			snapshot.get(LOBBY).toArray(lobbyUsers);
			redisTemplate.opsForSet().add(LOBBY, lobbyUsers);
		}
				
		String[] gameUsers = new String[snapshot.get(GAME).size()];
		if(gameUsers.length > 0) {
			snapshot.get(GAME).toArray(gameUsers);
			redisTemplate.opsForSet().add(GAME, gameUsers);
		}
					
		String[] loggedOutUsers = new String[snapshot.get(LOGGED_OUT).size()];
		if(loggedOutUsers.length > 0) {
			snapshot.get(LOGGED_OUT).toArray(loggedOutUsers);
			redisTemplate.opsForSet().add(LOGGED_OUT, loggedOutUsers);
		}
		
		readWriteLock.writeLock().unlock();				
	}
	
	/**
	 * calculate instance after events fold, will return this instance without saving it into Redis.
	 * @return 
	 */
	public Map<String,Set<String>> getInstanceFromEventsFold(LinkedList<BackgammonEvent> fromMongoEventsStoreEventList){
		boolean isLatestSnapshotExists = this.readLatestSnapshot() == null ? false : true;
		Map<String,Set<String>> currentSnapshot;
		
		if(isLatestSnapshotExists) currentSnapshot = this.readLatestSnapshot();
		else {
			currentSnapshot = new HashMap<>(10000);
			currentSnapshot.put(LOBBY, new HashSet<>());
			currentSnapshot.put(GAME, new HashSet<>());
			currentSnapshot.put(LOGGED_OUT, new HashSet<>());
		}

		ListIterator<BackgammonEvent> it = fromMongoEventsStoreEventList.listIterator();
		Map<String, BackgammonUser> aboutToEnterLobby = new HashMap<>(10000);
		
		logger.info("Starting to fold events into current state...");
		
		while(it.hasNext()){			
			BackgammonEvent eventToFold = it.next();
			logger.info("Event to fold = " + eventToFold);
			if(eventToFold.getClazz().equals("NewUserCreatedEvent")){
				NewUserCreatedEvent newUserCreatedEvent = (NewUserCreatedEvent) eventToFold;
				BackgammonUser backgammonUser  = newUserCreatedEvent.getBackgammonUser();
				aboutToEnterLobby.put(backgammonUser.getUserName(), backgammonUser);
			}
			else if(eventToFold.getClazz().equals("NewUserJoinedLobbyEvent")){
				NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = (NewUserJoinedLobbyEvent) eventToFold;
				BackgammonUser backgammonUser = newUserJoinedLobbyEvent.getBackgammonUser();
				if(aboutToEnterLobby.containsKey(backgammonUser.getUserName())){
					aboutToEnterLobby.remove(backgammonUser.getUserName());
				}
				ObjectMapper objectMapper = new ObjectMapper();
				String backgammonUserJson = null;
				try {
					backgammonUserJson = objectMapper.writeValueAsString(backgammonUser);
				} catch (JsonProcessingException e) {
					logger.error("Failed convert backgammon user to json...");
					logger.error(e.getMessage());
					e.printStackTrace();
				}
				currentSnapshot.get(LOBBY).add(backgammonUserJson);
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
