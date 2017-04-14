package org.moshe.arad;

import java.util.HashMap;
import java.util.Map;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.consumers.CreateNewUserCommandConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Users {

	@Autowired
	private CreateNewUserCommandConsumer createNewUserCommandConsumer;
	
	private Map<String,BackgammonUser> users = new HashMap<>(1000);
	
	public Users() {
	}
	
	public void acceptNewUsers(){
		System.out.println("Accept new users... start");
		createNewUserCommandConsumer.setUsers(users);
		createNewUserCommandConsumer.recieveKafkaMessage("Commands-To-Users-Service");
		System.out.println("Accept new users... end");
	}
}
