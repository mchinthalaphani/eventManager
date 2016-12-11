package com.charter.kafka.service.types;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "message")
public class Testmessage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@XmlElement
	public String id;
	
	@XmlElement
	public String name;

	public Testmessage(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public Testmessage() {
	}

	@Override
	public String toString() {
		return "Message{" + "id='" + id + '\'' + ", name=" + name + '}';
	}

}
