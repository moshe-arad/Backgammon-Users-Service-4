package org.moshe.arad.local.snapshot;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.entities.BackgammonUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

public class SnapshotAPI {

//	@Autowired
//	private RedisTemplate<String, String> redisTemplate;
	
	@Autowired
	private RedisTemplate<String, BackgammonUser> redisTemplate;
	
	@Autowired
	private StringRedisTemplate stringRedisTemplate;
	
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

	public Map<String,Set<BackgammonUser>> getLatestSnapshot(){
		if(!isLastUpdateSnapshotDateExists()) return null;
		else{
//			Set<String> usersInLobby = redisTemplate.opsForSet().members("Lobby");
			Set<BackgammonUser> usersInLobby = redisTemplate.opsForSet().members(LOBBY);
			Set<BackgammonUser> usersInGame = redisTemplate.opsForSet().members(GAME);
			Set<BackgammonUser> usersLoggedOut = redisTemplate.opsForSet().members(LOGGED_OUT);
			
			Map<String,Set<BackgammonUser>> result = new HashMap<String, Set<BackgammonUser>>(10000);
			
			result.put(LOBBY, usersInLobby);
			result.put(GAME, usersInGame);
			result.put(LOGGED_OUT, usersLoggedOut);
			return result;
		}
	}
	
	public void updateLatestSnapshot(Map<String,Set<BackgammonUser>> snapshot){
		redisTemplate.expire(LOBBY, 1, TimeUnit.NANOSECONDS);
		redisTemplate.expire(GAME, 1, TimeUnit.NANOSECONDS);
		redisTemplate.expire(LAST_UPDATED, 1, TimeUnit.NANOSECONDS);
		
		while(redisTemplate.hasKey(LOBBY) || redisTemplate.hasKey(GAME) || redisTemplate.hasKey(LOGGED_OUT)){
			Thread.sleep(1000);
		}
		
//		redisTemplate.opsForSet(). difference(LOBBY, snapshot.get(LOBBY));
		
		if(!isLastUpdateSnapshotDateExists()) return null;
		else{
//			Set<String> usersInLobby = redisTemplate.opsForSet().members("Lobby");
			Set<BackgammonUser> usersInLobby = redisTemplate.opsForSet().members(LOBBY);
			Set<BackgammonUser> usersInGame = redisTemplate.opsForSet().members(GAME);
			Set<BackgammonUser> usersLoggedOut = redisTemplate.opsForSet().members(LOGGED_OUT);
			
			Map<String,Set<BackgammonUser>> result = new HashMap<String, Set<BackgammonUser>>(10000);
			
			result.put(LOBBY, usersInLobby);
			result.put(GAME, usersInGame);
			result.put(LOGGED_OUT, usersLoggedOut);
			return result;
		}
	}
}
