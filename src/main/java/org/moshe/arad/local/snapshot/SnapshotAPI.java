package org.moshe.arad.local.snapshot;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.kafka.producers.commands.ISimpleCommandProducer;
import org.moshe.arad.kafka.producers.commands.PullEventsWithSavingCommandsProducer;
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
	
	private LinkedList<BackgammonEvent> fromMongoEventsStoreEventList = new LinkedList<>();
	
	private Logger logger = LoggerFactory.getLogger(SnapshotAPI.class);
	
	private Set<Object> updateSnapshotLocker = new HashSet<>(100000);
	
	private ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
	
	public static final String LAST_UPDATED = "lastUpdateSnapshotDate";
	public static final String LOBBY = "Lobby";
	public static final String GAME = "Game";
	public static final String LOGGED_OUT = "LoggedOut";
	
	public boolean isLastUpdateDateExists(){
		return redisTemplate.hasKey(LAST_UPDATED);
	}
	
	public Date getLastUpdateDate(){
		if(isLastUpdateDateExists()){
			return new Date(Long.parseLong(stringRedisTemplate.opsForValue().get(LAST_UPDATED)));
		}
		else return null;
	}

	public void saveLatestSnapshotDate(Date date){
		redisTemplate.opsForValue().set(LAST_UPDATED, Long.toString(date.getTime()));
	}
	
	public Map<String,Set<String>> readLatestSnapshot(){
		if(!isLastUpdateDateExists()) return null;
		else{
			Set<String> usersInLobby = redisTemplate.opsForSet().members(LOBBY);
			Set<String> usersInGame = redisTemplate.opsForSet().members(GAME);
			Set<String> usersLoggedOut = redisTemplate.opsForSet().members(LOGGED_OUT);
			
			Map<String,Set<String>> result = new HashMap<String, Set<String>>(10000);
			
			result.put(LOBBY, usersInLobby);
			result.put(GAME, usersInGame);
			result.put(LOGGED_OUT, usersLoggedOut);
			return result;
		}
	}	

	public Map<String,Set<String>> doEventsFoldingAndGetInstanceWithoutSaving(){		
		logger.info("Preparing command producer...");
		PullEventsWithSavingCommandsProducer pullEventsWithSavingCommandsProducer = context.getBean(PullEventsWithSavingCommandsProducer.class);
		initSingleProducer(pullEventsWithSavingCommandsProducer, KafkaUtils.PULL_EVENTS_WITHOUT_SAVING_COMMAND_TOPIC);	
		threadPoolExecutor.submit(pullEventsWithSavingCommandsProducer);
		logger.info("command submitted...");
		
		int counter = 0;
		
		while(this.fromMongoEventsStoreEventList.size() == 0){
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			counter++;
			if(counter == 40 && this.fromMongoEventsStoreEventList.size() == 0) return null;
		}
				 
		return getInstanceFromEventsFold();
	}
	
	private void initSingleProducer(ISimpleCommandProducer producer, String topic) {
		producer.setPeriodic(false);
		producer.setTopic(topic);
	}
	
	public void updateLatestSnapshot(Map<String,Set<String>> snapshot){
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
				
	}
	
	/**
	 * calculate instance after events fold, will return this instance without saving it into Redis.
	 * @return 
	 */
	private Map<String,Set<String>> getInstanceFromEventsFold(){
		boolean isLatestSnapshotExists = this.readLatestSnapshot() == null ? false : true;
		Map<String,Set<String>> currentSnapshot;
		
		if(isLatestSnapshotExists) currentSnapshot = this.readLatestSnapshot();
		else {
			currentSnapshot = new HashMap<>(10000);
			currentSnapshot.put(LOBBY, new HashSet<>());
			currentSnapshot.put(GAME, new HashSet<>());
			currentSnapshot.put(LOGGED_OUT, new HashSet<>());
		}

		ListIterator<BackgammonEvent> it = this.fromMongoEventsStoreEventList.listIterator();
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
	
	public LinkedList<BackgammonEvent> getFromMongoEventsStoreEventList() {
		return fromMongoEventsStoreEventList;
	}

	public void setFromMongoEventsStoreEventList(LinkedList<BackgammonEvent> fromMongoEventsStoreEventList) {
		this.fromMongoEventsStoreEventList = fromMongoEventsStoreEventList;
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
}
