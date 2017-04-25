package org.moshe.arad.local.snapshot;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class SnapshotAPI {

//	@Autowired
//	private RedisTemplate<String, String> redisTemplate;
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	@Autowired
	private StringRedisTemplate stringRedisTemplate;
	
	private LinkedList<BackgammonEvent> fromMongoEventsStoreEventList;
	
	private Logger logger = LoggerFactory.getLogger(SnapshotAPI.class);
	
	private static final String LAST_UPDATED = "lastUpdateSnapshotDate";
	private static final String LOBBY = "Lobby";
	private static final String GAME = "Lobby";
	private static final String LOGGED_OUT = "Lobby";
	
	public boolean isLastUpdateSnapshotDateExists(){
		return redisTemplate.hasKey(LAST_UPDATED);
	}
	
	public Date getLastUpdateSnapshotDate(){
		if(isLastUpdateSnapshotDateExists()){
			return new Date(stringRedisTemplate.opsForValue().get(LAST_UPDATED));
		}
		else return null;
	}

	public Map<String,Set<String>> getLatestSnapshot(){
		if(!isLastUpdateSnapshotDateExists()) return null;
		else{
//			Set<String> usersInLobby = redisTemplate.opsForSet().members("Lobby");
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
	
//	public void updateLatestSnapshot(Map<String,Set<BackgammonUser>> snapshot){
//		redisTemplate.expire(LOBBY, 1, TimeUnit.NANOSECONDS);
//		redisTemplate.expire(GAME, 1, TimeUnit.NANOSECONDS);
//		redisTemplate.expire(LAST_UPDATED, 1, TimeUnit.NANOSECONDS);
//		
//		while(redisTemplate.hasKey(LOBBY) || redisTemplate.hasKey(GAME) || redisTemplate.hasKey(LOGGED_OUT)){
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		
//		BackgammonUser[] lobbyUsers = new BackgammonUser[snapshot.get(LOBBY).size()];
//		snapshot.get(LOBBY).toArray(lobbyUsers);
//		
//		redisTemplate.opsForSet().add(LOBBY, lobbyUsers);
//		
//		BackgammonUser[] gameUsers = new BackgammonUser[snapshot.get(GAME).size()];
//		snapshot.get(GAME).toArray(gameUsers);
//		
//		redisTemplate.opsForSet().add(GAME, gameUsers);
//		
//		BackgammonUser[] loggedOutUsers = new BackgammonUser[snapshot.get(LOGGED_OUT).size()];
//		snapshot.get(LOGGED_OUT).toArray(loggedOutUsers);
//		
//		redisTemplate.opsForSet().add(LOGGED_OUT, loggedOutUsers);
//	}

	public void executeEventsFoldOnEventsFromMongo(){
//		Map<String,Set<BackgammonUser>> currentSnapshot = this.getLatestSnapshot();
		
		ListIterator<BackgammonEvent> it = this.fromMongoEventsStoreEventList.listIterator();
		Map<String, BackgammonUser> aboutToEnterLobby = new HashMap<>(10000);
		
		logger.info("Starting to fold events into current state...");
		
		while(it.hasNext()){			
			BackgammonEvent eventToFold = it.next();
			logger.info("Event to fold = " + eventToFold);
//			if(eventToFold.getClazz().equals(NewUserCreatedEvent.class)){
//				NewUserCreatedEvent newUserCreatedEvent = (NewUserCreatedEvent) eventToFold.getBackgammonEvent();
//				BackgammonUser backgammonUser  = newUserCreatedEvent.getBackgammonUser();
//				aboutToEnterLobby.put(backgammonUser.getUserName(), backgammonUser);
//			}
//			else if(eventToFold.getClazz().equals(NewUserJoinedLobbyEvent.class)){
//				NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = (NewUserJoinedLobbyEvent) eventToFold.getBackgammonEvent();
//				BackgammonUser backgammonUser = newUserJoinedLobbyEvent.getBackgammonUser();
//				if(aboutToEnterLobby.containsKey(backgammonUser.getUserName())){
//					aboutToEnterLobby.remove(backgammonUser.getUserName());
//					currentSnapshot.get(LOBBY).add(backgammonUser);
//				}
//				else{
//					currentSnapshot.get(LOBBY).add(backgammonUser);
//				}
//			}
//			logger.info("Event to folded successfuly = " + eventToFold);
			//TODO write code about logging out and in game code
		}
		
		logger.info("Events folding into current state completed...");
		logger.info("Updating current local redis snapshot...");
//		this.updateLatestSnapshot(currentSnapshot);
		logger.info("Current local redis snapshot update completed...");
	}
	public LinkedList<BackgammonEvent> getFromMongoEventsStoreEventList() {
		return fromMongoEventsStoreEventList;
	}

	public void setFromMongoEventsStoreEventList(LinkedList<BackgammonEvent> fromMongoEventsStoreEventList) {
		this.fromMongoEventsStoreEventList = fromMongoEventsStoreEventList;
	}	
}
