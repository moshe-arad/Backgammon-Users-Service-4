package org.moshe.arad.kafka;

public class KafkaUtils {

	public static final String SERVERS = "192.168.1.3:9092,192.168.1.3:9093,192.168.1.3:9094";
	public static final String CREATE_NEW_USER_COMMAND_GROUP = "CreateNewUserCommandGroup";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String CREATE_NEW_USER_COMMAND_DESERIALIZER = "org.moshe.arad.kafka.deserializers.CreateNewUserCommandDeserializer";
	public static final String NEW_USER_CREATED_EVENT_SERIALIZER = "org.moshe.arad.kafka.serializers.NewUserCreatedEventSerializer";
	public static final String COMMANDS_TO_USERS_SERVICE_TOPIC = "Commands-To-Users-Service";
	public static final String NEW_USER_CREATED_EVENT_TOPIC = "New-User-Created-Event";
	public static final String CHECK_USER_EMAIL_AVAILABILITY_COMMANDS_TOPIC = "Check-User-Email-Availability-Command";
	public static final String CHECK_USER_NAME_AVAILABILITY_COMMANDS_TOPIC = "Check-User-Name-Availability-Command";
	public static final String CHECK_USER_NAME_AVAILABILITY_GROUP = "CheckUserNameAvailabilityGroup";
	public static final String CHECK_USER_EMAIL_AVAILABILITY_COMMANDS_DESERIALIZER = "Check-User-Email-Availability-Command";
	public static final String CHECK_USER_NAME_AVAILABILITY_COMMANDS_DESERIALIZER = "Check-User-Name-Availability-Command";
	public static final String CREATE_NEW_USER_COMMAND_TOPIC = "Create-New-User-Command";
	public static final String PULL_EVENTS_COMMAND_TOPIC = "Pull-Events-Command";
	public static final String PULL_EVENTS_COMMAND_SERIALIZER = "org.moshe.arad.kafka.serializers.PullEventsCommandSerializer";
	public static final String FROM_MONGO_TO_USERS_SERVICES = "From-Mongo-To-Users-Service";
	public static final String FROM_MONGO_TO_USERS_SERVICE_GROUP = "FromMongoToUsersServiceGroup";
	public static final String FROM_MONGO_TO_USERS_SERVICE_DESERIALIZER = "org.moshe.arad.kafka.deserializers.FromMongoEventsStoreEventConfig";
}
