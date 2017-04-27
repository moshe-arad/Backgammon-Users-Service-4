package org.moshe.arad.local.snapshot;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class SnapshotAPI {
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	@Autowired
	private StringRedisTemplate stringRedisTemplate;
	
	private LinkedList<BackgammonEvent> fromMongoEventsStoreEventList;
	
	private Logger logger = LoggerFactory.getLogger(SnapshotAPI.class);
	
	private Date latestSnapshotDate;
	
	private static final String LAST_UPDATED = "lastUpdateSnapshotDate";
	private static final String LOBBY = "Lobby";
	private static final String GAME = "Game";
	private static final String LOGGED_OUT = "LoggedOut";
	
	public boolean isLastUpdateSnapshotDateExists(){
		return redisTemplate.hasKey(LAST_UPDATED);
	}
	
	public Date getLastUpdateSnapshotDate(){
		if(isLastUpdateSnapshotDateExists()){
			return new Date(Long.parseLong(stringRedisTemplate.opsForValue().get(LAST_UPDATED)));
		}
		else return null;
	}

	public Map<String,Set<String>> getLatestSnapshot(){
		if(!isLastUpdateSnapshotDateExists()) return null;
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

	public void executeEventsFoldOnEventsFromMongo(){
		boolean isLatestSnapshotExists = this.getLatestSnapshot()==null ? false:true;
		Map<String,Set<String>> currentSnapshot;
		
		if(isLatestSnapshotExists) currentSnapshot = this.getLatestSnapshot();
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
		logger.info("Updating current local redis snapshot...");
		
		try{
			if(fromMongoEventsStoreEventList.size() > 0){
				this.updateLatestSnapshot(currentSnapshot);
				redisTemplate.opsForValue().set(LAST_UPDATED, Long.toString(this.latestSnapshotDate.getTime()));
			}			
		}
		catch (Exception e) {
			logger.error("Failed to update redis snapshot...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
		logger.info("Current local redis snapshot update completed...");
	}
	public LinkedList<BackgammonEvent> getFromMongoEventsStoreEventList() {
		return fromMongoEventsStoreEventList;
	}

	public void setFromMongoEventsStoreEventList(LinkedList<BackgammonEvent> fromMongoEventsStoreEventList) {
		this.fromMongoEventsStoreEventList = fromMongoEventsStoreEventList;
	}

	public Date getLatestSnapshotDate() {
		return latestSnapshotDate;
	}

	public void setLatestSnapshotDate(Date latestSnapshotDate) {
		this.latestSnapshotDate = latestSnapshotDate;
	}
}
