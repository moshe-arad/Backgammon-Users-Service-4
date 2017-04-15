package org.moshe.arad.kafka.deserializers;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.moshe.arad.Location;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.commands.CreateNewUserCommand;

public class CreateNewUserCommandDeserializer implements Deserializer<CreateNewUserCommand>{
	private String encoding = "UTF8";
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CreateNewUserCommand deserialize(String topic, byte[] data) {
		try {
            if (data == null){
                System.out.println("Null recieved at deserialize");
                return null;
            }
            
            ByteBuffer buf = ByteBuffer.wrap(data);         
         
            String userName = DeserializeString(buf);
            String password = DeserializeString(buf);
            String firstName = DeserializeString(buf);
            String lastName = DeserializeString(buf);
            String email = DeserializeString(buf);       
            
            return new CreateNewUserCommand(
            		new BackgammonUser(userName, password, firstName, lastName, email, Location.Lobby));            		           
            
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to CreateNewUserCommand");
        }
	}

	private String DeserializeString(ByteBuffer buf) throws UnsupportedEncodingException {
		int sizeOfUserName = buf.getInt();
		byte[] nameBytes = new byte[sizeOfUserName];
		buf.get(nameBytes);
		String deserializedUserName = new String(nameBytes, encoding);
		return deserializedUserName;
	}

}
