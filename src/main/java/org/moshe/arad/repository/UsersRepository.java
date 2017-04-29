package org.moshe.arad.repository;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.PostConstruct;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.producers.commands.ISimpleCommandProducer;
import org.moshe.arad.kafka.producers.commands.PullEventsCommandsProducer;
import org.moshe.arad.kafka.producers.config.SimpleProducerConfig;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Repository
public class UsersRepository implements ApplicationContextAware {

	@Autowired
	private SnapshotAPI snapshotAPI;
	
	@Autowired
	private ApplicationContext context;
	
	private ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
	
	private Logger logger = LoggerFactory.getLogger(UsersRepository.class);
	
	public UsersRepository() {
	}
	
	@PostConstruct
	public void initUpdateSnapshotLocker(){
		snapshotAPI.getUpdateSnapshotLocker().add(pullEventsCommandsProducer);
	}
	
	public boolean isUserExists(BackgammonUser user) throws InterruptedException {
		logger.info("Preparing command producer...");
		PullEventsCommandsProducer pullEventsCommandsProducer = context.getBean(PullEventsCommandsProducer.class);
		initSingleProducer(pullEventsCommandsProducer, KafkaUtils.PULL_EVENTS_COMMAND_TOPIC);		
		
		Future<UUID> uuidFuture = threadPoolExecutor.submit(pullEventsCommandsProducer);
		logger.info("command submitted...");
		
		Thread currentThread = Thread.currentThread();
		
		try {		
			snapshotAPI.getUsersLockers().put(uuidFuture.get(), currentThread);
		} catch (InterruptedException | ExecutionException e1) {
			e1.printStackTrace();
		}
		
		synchronized (currentThread) {
			try {
				currentThread.wait(2000);
				Map<String, Set<String>> snapshot = snapshotAPI.getTempSnapshot();
				
				if(isUserExistsInSnapshot(user, snapshot)) return true;
				else return false;
			} catch (InterruptedException e) {
				logger.error("Failed to get current snapshot...");
				logger.error(e.getMessage());
				e.printStackTrace();
				throw e;
			}
		}						
	}
	
	
	private void initSingleProducer(ISimpleCommandProducer producer, String topic) {
		producer.setPeriodic(false);
		producer.setToSaveEvent(false);
		producer.setTopic(topic);
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

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}
}
