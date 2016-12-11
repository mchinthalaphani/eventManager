package com.charter.kafka.service.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;


@SpringBootApplication
@ComponentScan({"com.charter.kafka.*","com.charter.solosync.db.*"})
@EnableJpaRepositories("com.charter.solosync.db.repository")
@EntityScan("com.charter.solosync.db.model")
@PropertySource({"classpath:application.properties","dbconnector.properties"})
public class KafkaApplication 
{
	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}
}
