package org.moshe.arad.repository;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Repository
public class UsersRepository {

	@Autowired
	private SnapshotAPI snapshotAPI;	
	
	private Logger logger = LoggerFactory.getLogger(UsersRepository.class);
	
	public UsersRepository() {
	}
	
	public boolean isUserExists(BackgammonUser user) {
		
		Map<String,Set<String>> snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		
		if(snapshot == null) return false;
		else return isUserExistsInSnapshot(user, snapshot);				
	}
		
	private boolean isUserExistsInSnapshot(BackgammonUser user, Map<String, Set<String>> snapshot){
		ObjectMapper objectMapper = new ObjectMapper();
		Set<String> setsUnion = new HashSet<>(100000);
		
		setsUnion.addAll(snapshot.get(SnapshotAPI.LOBBY));
		setsUnion.addAll(snapshot.get(SnapshotAPI.GAME));
		setsUnion.addAll(snapshot.get(SnapshotAPI.LOGGED_OUT));
		
		Iterator<String> it = setsUnion.iterator();
		
		while(it.hasNext()){
			try {
				JsonNode jsonNode = objectMapper.readValue(it.next(), JsonNode.class);
				String userName = jsonNode.get("userName").asText();
				String email = jsonNode.get("email").asText();
				
				if(userName.equals(user.getUserName()) && email.equals(user.getEmail())) return true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return false;
	}
}
