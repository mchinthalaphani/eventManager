package com.charter.kafka.service.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class XmlEncoder implements Encoder<Object> {
	
	  public XmlEncoder(VerifiableProperties verifiableProperties) {
	        /* This constructor must be present for successful compile. */
	  }
	  public XmlEncoder() {
	        /* This constructor must be present for successful compile. */
	  }

	@Override
	public byte[] toBytes(Object object) {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o;
		try {
			o = new ObjectOutputStream(b);
			o.writeObject(object);
		} catch (IOException e) {
			System.out.println("Error while converting ...");
			e.printStackTrace();
		}
		return b.toByteArray();
	}

}
