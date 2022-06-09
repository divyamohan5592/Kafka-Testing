package com.stinsoft.kafka;

import com.stinsoft.kafka.kafka.KafkaProcessingEventsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.Timestamp;

@SpringBootApplication
public class KafkaApplication{

	// Our inbound messages to API server.
	static protected KafkaProcessingEventsProducer s_kafkaConsumerInbound;

	static Logger logger = LoggerFactory.getLogger(KafkaApplication.class);


	public static void main(String[] args) {
		initializeKafka();
		SpringApplication.run(KafkaApplication.class, args);
	}

	public static void initializeKafka()
	{
		try
		{
			String kafkaConnection = "192.168.78.35:9092";
			if (kafkaConnection != null)
			{
				// If we found a kafka server in our environment, initialize producer/consumers now.

				s_kafkaConsumerInbound = new KafkaProcessingEventsProducer(kafkaConnection);

				int i = 1;

				for (; ; )
				{
					Thread.sleep(60000);

					Timestamp systemTime = new Timestamp(System.currentTimeMillis());

					s_kafkaConsumerInbound.sendADLBaselineAlert(systemTime, i, i, i, i);

					i++;
				}

			}
		}
		catch (Exception ex)
		{
			logger.error("error", ex);
		}
	}
}
