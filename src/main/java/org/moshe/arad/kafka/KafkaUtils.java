package org.moshe.arad.kafka;

public class KafkaUtils {

	public static final String SERVERS = "192.168.1.10:9092,192.168.1.10:9093,192.168.1.10:9094";
	public static final String CREATE_NEW_USER_COMMAND_GROUP = "CreateNewUserCommandGroup";
	public static final String KEY_STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String KEY_STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String CREATE_NEW_USER_COMMAND_DESERIALIZER = "org.moshe.arad.kafka.deserializers.CreateNewUserCommandDeserializer";
	public static final String NEW_USER_CREATED_EVENT_SERIALIZER = "org.moshe.arad.kafka.serializers.NewUserCreatedEventSerializer";
}
