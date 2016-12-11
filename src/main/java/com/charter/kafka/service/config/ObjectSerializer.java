package com.charter.kafka.service.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.charter.eni.model.Msg;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectSerializer implements Serializer<Object> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] serialize(String topic, Object data) {

//		byte[] retval = null;
//		ObjectMapper objectMapper = new ObjectMapper();
//		try {
//			retval = objectMapper.writeValueAsString(data).getBytes();
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}
//		return retval;
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o;
		try {
			o = new ObjectOutputStream(b);
			o.writeObject(data);
		} catch (IOException e) {
			System.out.println("Error while converting ...");
			e.printStackTrace();
		}
		return b.toByteArray();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
