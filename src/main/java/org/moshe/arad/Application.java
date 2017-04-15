package org.moshe.arad;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.annotation.RequestScope;

@SpringBootApplication
@RestController
public class Application implements ApplicationRunner, ApplicationContextAware {

	@Autowired
	private Users users;
	private ApplicationContext context;
	private Logger logger = LoggerFactory.getLogger(Application.class);
	
	
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
		
	}

	@Override
	public void run(ApplicationArguments arg0) throws Exception {
		users.setContext(context);
		users.acceptNewUsers();
	}
	
	@RequestMapping("/shutdown")
	public ResponseEntity<String> shutdown(){
		try{
			logger.info("about to do shutdown.");
			users.shutdown();
			logger.info("shutdown compeleted.");
			return new ResponseEntity<String>("", HttpStatus.OK);
		}
		catch(Exception ex){
			logger.info("Failed to shutdown users service.");
			return new ResponseEntity<String>("", HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}
}
