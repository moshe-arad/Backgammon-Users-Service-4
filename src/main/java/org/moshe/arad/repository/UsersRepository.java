package org.moshe.arad.repository;

import java.io.IOException;
import java.util.Map;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;

@Repository
public class UsersRepository {

	@Autowired
	private SnapshotAPI snapshotAPI;	
	
	private Logger logger = LoggerFactory.getLogger(UsersRepository.class);
	
	public UsersRepository() {
	}
	
	public boolean isUserExists(BackgammonUser user) {
		Map<String,Map<Object, Object>> snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		
		if(snapshot == null) return false;
		else return isUserExistsInSnapshot(user, snapshot);			
	}
	
	public BackgammonUser isUserExistsAndReturn(BackgammonUser user) {
		Map<String,Map<Object, Object>> snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		BackgammonUser result = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			if(snapshot.get(SnapshotAPI.USERS) != null){
				result = objectMapper.readValue(snapshot.get(SnapshotAPI.USERS).get(user.getUserName()).toString(), BackgammonUser.class);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	private boolean isUserExistsInSnapshot(BackgammonUser user, Map<String,Map<Object, Object>> snapshot){
		if(snapshot.get(SnapshotAPI.USERS).containsKey(user.getUserName())) return true;
		return false;		
	}
}
