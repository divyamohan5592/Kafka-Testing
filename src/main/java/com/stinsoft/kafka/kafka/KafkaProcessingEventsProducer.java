package com.stinsoft.kafka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.util.Properties;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProcessingEventsProducer{

    final private static String KAFKA_PRODUCER_TOPIC = "testingEvents";

    protected Producer<String, String> m_producer;

    protected static KafkaProcessingEventsProducer s_thisProducer;

    static Logger logger = LoggerFactory.getLogger(KafkaProcessingEventsProducer.class);


    public KafkaProcessingEventsProducer(String serverPath)
    {
        try
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", serverPath);

            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            m_producer = new KafkaProducer<>(props);
        }
        catch (Exception ex)
        {
            logger.error("Kafka Processing Inbound Setup", ex);
        }

        s_thisProducer = this;
    }



    static public void sendADLBaselineAlert(Timestamp gatewayUTC, int alertBaselineHistoryID, int databaseInstanceID,
                                            int consumerAccountID, int accountResidentID)
    {
        JSONObject adlBaselineAlert = new JSONObject();

        long gatewaySecs = (gatewayUTC.getTime() / 1000L);

        // Changing device UTC to be system UTC due to inaccurate device clocks.
        adlBaselineAlert.put("gatewayUTC", gatewaySecs);
        adlBaselineAlert.put("alertBaselineHistoryID", alertBaselineHistoryID);
        adlBaselineAlert.put("databaseInstanceID", databaseInstanceID);
        adlBaselineAlert.put("consumerAccountID", consumerAccountID);
        adlBaselineAlert.put("accountResidentID", accountResidentID);

        if (s_thisProducer != null)
        {
            s_thisProducer.sendKafkaMessage("testKafkaMessage", adlBaselineAlert.toString());
        }
        else
        {
            logger.error("Cannot Send Kafka ADLAlert : " + adlBaselineAlert.toString());
        }
    }


    public void sendKafkaMessage(String key, String value)
    {
        try
        {
            if (m_producer != null)
            {
                logger.info("Sending Consumer Kafka : " + key + " (" + value + ")");

                m_producer.send(new ProducerRecord<String, String>(KAFKA_PRODUCER_TOPIC, key, value));
            }
            else
            {
                logger.error("Cannot Send Kafka Message : " + key + " (" + value + ")");
            }
        }
        catch (Exception ex)
        {
            logger.error("Kafka Consumer Outbound Send", ex);
        }
    }

    public void shutdown()
    {
        try
        {
            m_producer.close();
        }
        catch (Exception ex)
        {
            logger.error("Kafka Consumer Outbound Close", ex);
        }
    }

}
