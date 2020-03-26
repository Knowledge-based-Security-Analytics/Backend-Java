package com.ur.ifs;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.render.JSONEventRenderer;
import com.espertech.esper.common.internal.collection.TransformEventIterator;
import com.espertech.esper.common.internal.epl.annotation.AnnotationUtil;
import com.espertech.esper.runtime.client.*;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowController;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowControllerContext;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/*Publiziert den Output aller Statements, die mit der @KafkaOutput('String TopicsCSV') Annotation gekennzeichnet wurden
an die in der Annotation enthaltenen Topics*/
public class EsperIOKafkaOutputFlowControllerCustom implements EsperIOKafkaOutputFlowController {

    private static final Logger LOG = LoggerFactory.getLogger(EsperIOKafkaOutputFlowControllerCustom.class);

    private KafkaProducer producer;
    private EPRuntime runtime;

    public void initialize(EsperIOKafkaOutputFlowControllerContext context) {
        this.runtime = context.getRuntime();

        // obtain producer
        try {
            producer = new KafkaProducer<>(context.getProperties());
        } catch (Throwable t) {
            LOG.error("Error obtaining Kafka producer for URI '{}': {}", context.getRuntime().getURI(), t.getMessage(), t);
        }

        // attach to existing statements
        String[] deploymentIds = context.getRuntime().getDeploymentService().getDeployments();
        for (String depoymentId : deploymentIds) {
            EPStatement[] statements = context.getRuntime().getDeploymentService().getDeployment(depoymentId).getStatements();
            for (EPStatement statement : statements) {
                processStatement(statement);

            }
        }

        // attach listener to receive newly-created statements
        runtime.getDeploymentService().addDeploymentStateListener(new DeploymentStateListener() {
            public void onDeployment(DeploymentStateEventDeployed event) {
                for (EPStatement statement : event.getStatements()) {
                    processStatement(statement);
                }
            }

            public void onUndeployment(DeploymentStateEventUndeployed event) {
                for (EPStatement statement : event.getStatements()) {
                    detachStatement(statement);
                }
            }
        });
    }

    private void processStatement(EPStatement statement) {
        if (statement == null) {
            return;
        }
        Annotation annotation = AnnotationUtil.findAnnotation(statement.getAnnotations(), KafkaOutput.class);
        if (annotation == null) {
            return;
        }
        Set<String> topics = new LinkedHashSet<>();
        String topicsCSV = (((KafkaOutput) annotation).value());
        String[] topicNames = topicsCSV.split(",");
        for (String topicName : topicNames) {
            if (topicName.trim().length() > 0) {
                topics.add(topicName.trim());
            }
        }
        KafkaOutputDefaultListener listener = new KafkaOutputDefaultListener(runtime, statement, producer, topics);
        statement.addListener(listener);
        LOG.info("Added Kafka-Output-Adapter listener to statement '{}' topics {}", statement.getName(), topics.toString());
    }

    private void detachStatement(EPStatement statement) {
        Iterator<UpdateListener> listeners = statement.getUpdateListeners();
        UpdateListener found = null;
        while (listeners.hasNext()) {
            UpdateListener listener = listeners.next();
            if (listener instanceof KafkaOutputDefaultListener) {
                found = listener;
                break;
            }
        }
        if (found != null) {
            statement.removeListener(found);
        }
        LOG.info("Removed Kafka-Output-Adapter listener from statement '{}'", statement.getName());
    }

    public void close() {
        producer.close();
    }

    public static class KafkaOutputDefaultListener implements UpdateListener {

        private final JSONEventRenderer jsonEventRenderer;
        private final KafkaProducer producer;
        private final Set<String> topics;

        public KafkaOutputDefaultListener(EPRuntime runtime, EPStatement statement, KafkaProducer producer, Set<String> topics) {
            jsonEventRenderer = runtime.getRenderEventService().getJSONRenderer(statement.getEventType());
            this.producer = producer;
            this.topics = topics;
        }

        public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
            if (newEvents == null) {
                return;
            }
            for (EventBean event : newEvents) {
                String json = jsonEventRenderer.render(event);
                JsonObject originalEventJson = new Gson().fromJson(json, JsonObject.class);
                JsonObject outputEventJson = originalEventJson.deepCopy();
                JsonArray sources = new JsonArray();
                for (Map.Entry<String, JsonElement> entry: originalEventJson.entrySet()) {
                    if(entry.getKey().contains("source_") && entry.getValue().isJsonObject()) {
                        sources.add(entry.getValue());
                        outputEventJson.remove(entry.getKey());
                    }
                }
                outputEventJson.addProperty("complex", true);
                outputEventJson.addProperty("timestamp", Instant.now().toEpochMilli());
                outputEventJson.addProperty("id", UUID.randomUUID().toString());
                outputEventJson.add("sources", sources);
                json = new Gson().toJson(outputEventJson);
                for (String topic : topics) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Producing event " + json + "to topic " + topic);
                    }
                    producer.send(new ProducerRecord(topic, json));
                }
            }
        }
    }
}
