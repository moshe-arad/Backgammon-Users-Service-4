package org.moshe.arad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.ConsumerToProducerJob;
import org.moshe.arad.kafka.consumers.CreateNewUserCommandConsumer;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class Users {

	private ApplicationContext context;
	private Map<String,BackgammonUser> users = new HashMap<>(1000);
	
	@Autowired
	private CreateNewUserCommandConsumer createNewUserCommandConsumer;
	
	private static final int CONSUMERS_NUM = 3;
//	private List<CreateNewUserCommandConsumer> createNewUserCommandConsumer = new ArrayList<>(CONSUMERS_NUM);
//	private ExecutorService createNewUserCommandConsumersPool = Executors.newFixedThreadPool(CONSUMERS_NUM);
	
	private static final int PRODUCERS_NUM = 3;
	private ScheduledThreadPoolExecutor newUserCreatedEventProducersPool = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(PRODUCERS_NUM);
	private List<ConsumerToProducerJob> consumerToProducerJobs = new ArrayList<>(PRODUCERS_NUM);
	
	private Logger logger = LoggerFactory.getLogger(Users.class);
	
	public Users() {
	}
	
	public void acceptNewUsers(){
		logger.info("Accept new users... start");
		createNewUserCommandConsumer.executeConsumers(CONSUMERS_NUM);
		logger.info("Accept new users... end");
		
		logger.info("Start consumer to producer jobs...");
	}
	
	public void shutdown(){
		createNewUserCommandConsumer.setRunning(false);
		createNewUserCommandConsumer.getScheduledExecutor().shutdown();
	}

	public ApplicationContext getContext() {
		return context;
	}

	public void setContext(ApplicationContext context) {
		this.context = context;
	}
}
