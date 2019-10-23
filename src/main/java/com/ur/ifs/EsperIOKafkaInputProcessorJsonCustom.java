package com.ur.ifs;

import com.espertech.esper.common.client.EPException;
import com.espertech.esper.common.internal.util.JavaClassHelper;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esperio.kafka.EsperIOKafkaConfig;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessor;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessorContext;
import com.espertech.esperio.kafka.EsperIOKafkaInputTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*Sendet von Kafka Topics konsumierte JSON-Events mit Eventtyp = TopicName an die Runtime*/
public class EsperIOKafkaInputProcessorJsonCustom implements EsperIOKafkaInputProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(EsperIOKafkaInputProcessorJsonCustom.class);

    private EPRuntime runtime;
    private EsperIOKafkaInputTimestampExtractor timestampExtractor;
    private String eventTypeName;

    public void init(EsperIOKafkaInputProcessorContext context) {
        this.runtime = context.getRuntime();

        String timestampExtractorClassName = context.getProperties().getProperty(EsperIOKafkaConfig.INPUT_TIMESTAMPEXTRACTOR_CONFIG);
        if (timestampExtractorClassName != null) {
            timestampExtractor = (EsperIOKafkaInputTimestampExtractor) JavaClassHelper.instantiate(EsperIOKafkaInputTimestampExtractor.class, timestampExtractorClassName, context.getRuntime().getServicesContext().getClasspathImportServiceRuntime().getClassForNameProvider());
        }
    }

    public void process(ConsumerRecords<Object, Object> records) {
        for (ConsumerRecord record : records) {
            eventTypeName = record.topic();
            if (blockingCheckEventType()) {

                if (timestampExtractor != null) {

                    long timestamp = timestampExtractor.extract(record);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sending time {}", timestamp);
                    }
                    if (timestamp != 0) {
                        runtime.getEventService().advanceTime(timestamp);
                    }
                }

                if (record.value() != null) {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sending event of type " + eventTypeName + " {}", record.value().toString());
                    }
                    String json = record.value().toString();
                    try {
                        runtime.getEventService().sendEventJson(json, eventTypeName);
                    } catch (EPException ex) {
                        LOG.error("Exception processing message: " + ex.getMessage(), ex);
                    }
                }
            }
        }
    }

    private boolean blockingCheckEventType() {

        boolean found = runtime.getEventTypeService().getBusEventType(eventTypeName) != null;
        if (found) return true;
        LOG.info("Could not find json event type '" + eventTypeName + "', the type has not been defined or does not have bus-visibility --> Event discarded");
        return false;
    }


    public void close() {

    }
}
