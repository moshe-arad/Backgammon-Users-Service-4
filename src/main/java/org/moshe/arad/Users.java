package org.moshe.arad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.consumers.CreateNewUserCommandConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class Users {

	private ApplicationContext context;
	private List<CreateNewUserCommandConsumer> createNewUserCommandConsumer = new ArrayList<>(CONSUMERS_NUM);
	private Map<String,BackgammonUser> users = new HashMap<>(1000);
	private ExecutorService createNewUserCommandConsumersPool = Executors.newFixedThreadPool(CONSUMERS_NUM);
	private static final int CONSUMERS_NUM = 6;
	private Logger logger = LoggerFactory.getLogger(Users.class);
	
	public Users() {
	}
	
	public void acceptNewUsers(){
		for(int i=0; i<CONSUMERS_NUM; i++){
			createNewUserCommandConsumer.add(context.getBean(CreateNewUserCommandConsumer.class));
			createNewUserCommandConsumer.get(i).setUsers(users);
		}
		
		logger.info("Accept new users... start");
		try {
			createNewUserCommandConsumersPool.invokeAll(createNewUserCommandConsumer);
		} catch (InterruptedException e) {
			logger.error("Failed to start Create New User Command Consumers Pool.");
			e.printStackTrace();
		}
		logger.info("Accept new users... end");
	}
	
	public void shutdown(){
		createNewUserCommandConsumersPool.shutdown();
	}

	public ApplicationContext getContext() {
		return context;
	}

	public void setContext(ApplicationContext context) {
		this.context = context;
	}
}
