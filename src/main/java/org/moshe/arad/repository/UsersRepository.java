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
		
		long startTime = System.nanoTime();
		Map<String,Map<Object, Object>> snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		long endTime = System.nanoTime();

		long duration = (endTime - startTime);
		logger.info("***************************************");
		logger.info("***************************************");
		logger.info("*******  duration = "+ duration+" *********");
		logger.info("***************************************");
		
		
		if(snapshot == null) return false;
		else {
			long startTime1 = System.nanoTime();
			boolean ans = isUserExistsInSnapshot(user, snapshot);			
			long endTime1 = System.nanoTime();

			long duration1 = (endTime1 - startTime1);
			logger.info("***************************************");
			logger.info("***************************************");
			logger.info("*******  duration = "+ duration1+" *********");
			logger.info("***************************************");
			
			return ans;
		}
	}
		
	private boolean isUserExistsInSnapshot(BackgammonUser user, Map<String,Map<Object, Object>> snapshot){
		if(snapshot.get(SnapshotAPI.USERS).containsKey(user.getUserName())) return true;
		return false;		
	}
}
