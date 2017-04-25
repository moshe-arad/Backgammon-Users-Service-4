package org.moshe.arad.kafka.deserializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.moshe.arad.kafka.events.FromMongoEventsStoreEvent;

public class FromMongoEventsStoreEventConfig implements Deserializer<FromMongoEventsStoreEvent>{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FromMongoEventsStoreEvent deserialize(String arg0, byte[] arg1) {
		// TODO Auto-generated method stub
		return null;
	}

//	private String encoding = "UTF8";
//	
//	@Override
//	public void close() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void configure(Map<String, ?> arg0, boolean arg1) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public FromMongoEventsStoreEvent deserialize(String topic, byte[] data) {
//		try {
//            if (data == null){
//                System.out.println("Null recieved at deserialize");
//                return null;
//            }
//            
//            ByteBuffer buf = ByteBuffer.wrap(data);         
//         
//            String userName = deserializeString(buf);
//            String password = deserializeString(buf);
//            String firstName = deserializeString(buf);
//            String lastName = deserializeString(buf);
//            String email = deserializeString(buf);       
//            UUID uuid = new UUID(buf.getLong(), buf.getLong());
//            
//            return new CreateNewUserCommand(uuid,
//            		new BackgammonUser(userName, password, firstName, lastName, email, Location.Lobby));            		           
//            
//        } catch (Exception e) {
//            throw new SerializationException("Error when deserializing byte[] to CreateNewUserCommand");
//        }
//	}
//
//	private String deserializeString(ByteBuffer buf) throws UnsupportedEncodingException {
//		int sizeOfStringDeserialize = buf.getInt();
//		byte[] nameBytes = new byte[sizeOfStringDeserialize];
//		buf.get(nameBytes);
//		String deserializedString = new String(nameBytes, encoding);
//		return deserializedString;
//	}
}
