package org.moshe.arad.kafka.serializers;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.moshe.arad.kafka.commands.PullEventsCommand;

public class PullEventsCommandSerializer implements Serializer<PullEventsCommand>{

private static final String encoding = "UTF8";
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String arg0, PullEventsCommand event) {		
		 try {
			 if (event == null)
				 return null;
			 
			long highUuid = event.getUuid().getMostSignificantBits();
			long lowUuid = event.getUuid().getLeastSignificantBits();
			long fromDate = event.getFromDate().getTime();
			
			ByteBuffer buf = ByteBuffer.allocate(8+8+8);			                        
             
            buf.putLong(highUuid);
            buf.putLong(lowUuid);
            buf.putLong(fromDate);
             
	         return buf.array();

	        } catch (Exception e) {
	            throw new SerializationException("Error when serializing PullEventsCommand to byte[]");
	        }
	}
}
