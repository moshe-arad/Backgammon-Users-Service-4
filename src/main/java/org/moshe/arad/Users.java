package org.moshe.arad;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.consumers.CreateNewUserCommandConsumer;
import org.moshe.arad.kafka.producers.SimpleProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class Users {

	private Map<String,BackgammonUser> users = new HashMap<>(1000);
	
	@Autowired
	private CreateNewUserCommandConsumer createNewUserCommandConsumer;

	@Autowired
	private SimpleProducer simpleProducer;
	
	private ExecutorService executor = Executors.newFixedThreadPool(4);
	
	
	private Logger logger = LoggerFactory.getLogger(Users.class);
	
	public Users() {
	}
	
	public void acceptNewUsers(){
		logger.info("Accept new users... start");
		executor.execute(createNewUserCommandConsumer);
		executor.execute(simpleProducer);
		logger.info("Accept new users... end");
	}
	
	public void shutdown(){
		createNewUserCommandConsumer.setRunning(false);
		createNewUserCommandConsumer.getScheduledExecutor().shutdown();
		
		simpleProducer.setRunning(false);
		simpleProducer.getScheduledExecutor().shutdown();
		
		this.executor.shutdown();
	}
}
