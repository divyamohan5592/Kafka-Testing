package com.stinsoft.kafka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaAdminDataProducer {

    final private static String             KAFKA_PRODUCER_TOPIC = "testEventInbound";

    protected Producer<String, String>      m_producer;

    protected static KafkaAdminDataProducer s_thisProducer;

    protected static int                    s_syntheticLogCount = 10;

    static Logger logger = LoggerFactory.getLogger(KafkaProcessingEventsProducer.class);


    public KafkaAdminDataProducer(String serverPath)
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
            logger.error("Kafka Enterprise Inbound Setup", ex);
        }

        s_thisProducer = this;
    }


    static public void sendEventFindTempoClear(int facilityID, int tempoID, int beaconID)
    {
        JSONObject findTempoClear = new JSONObject();

        findTempoClear.put("facilityID", facilityID);
        findTempoClear.put("tempoID", tempoID);
        findTempoClear.put("beaconID", beaconID);

        if (s_thisProducer != null)
        {
            s_thisProducer.sendKafkaMessage("testFindTempoClear", findTempoClear.toString(), false);
        }
        else
        {
            logger.error("Cannot Send Kafka : FindTempoClear : " + findTempoClear.toString());
        }
    }

    static public void sendEventWiFiMap(int facilityID, int floorID, int mapX, int mapY, int rssi, int tempoWifiSampleID, int requestID)
    {
        JSONObject wifiMapValues = new JSONObject();

        wifiMapValues.put("facilityID", Long.toString(facilityID));
        wifiMapValues.put("floorID", Long.toString(floorID));
        wifiMapValues.put("mapX", Integer.toString(mapX));
        wifiMapValues.put("mapY", Integer.toString(mapY));
        wifiMapValues.put("rssi", Integer.toString(rssi));
        wifiMapValues.put("tempoWifiSampleID", Integer.toString(tempoWifiSampleID));
        wifiMapValues.put("requestID", Integer.toString(requestID));

        if (s_thisProducer != null)
        {
            s_thisProducer.sendKafkaMessage("WiFiSignalComplete", wifiMapValues.toString(), false);
        }
        else
        {
            logger.error("Cannot Send Kafka : WiFiSignalComplete : " + wifiMapValues.toString());
        }
    }


    synchronized private void sendKafkaMessage(String key, String value, boolean isSynthetic)
    {
        try
        {
            if (m_producer != null)
            {
                // Only log the first few synthetic alerts.

                if (isSynthetic == false || s_syntheticLogCount > 0)
                {
                    logger.info("Sending Kafka Message : " + key + " (" + value + ")");
                    s_syntheticLogCount--;
                }

                m_producer.send(new ProducerRecord<String, String>(KAFKA_PRODUCER_TOPIC, key, value));
            }
            else
            {
                logger.error("Cannot Send Kafka Message : " + key + " (" + value + ")");
            }
        }
        catch (Exception ex)
        {
            logger.error("Kafka Producer Send", ex);
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
            logger.error("Kafka Producer Close", ex);
        }
    }
}
