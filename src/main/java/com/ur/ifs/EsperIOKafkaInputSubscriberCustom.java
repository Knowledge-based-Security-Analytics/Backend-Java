package com.ur.ifs;

import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriber;
import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriberContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/*Abboniert alle Kafka Topics die mit dem String viscep_event_ beginnen*/
public class EsperIOKafkaInputSubscriberCustom implements EsperIOKafkaInputSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(EsperIOKafkaInputSubscriberCustom.class);

    public void subscribe(EsperIOKafkaInputSubscriberContext context) {
        LOG.info("Subscribing to all Kafka Topics beginning with viscep_event_");
        Pattern matchAll = Pattern.compile("viscep_event_.*");
        context.getConsumer().subscribe(matchAll);
    }
}
