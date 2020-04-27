package com.ur.ifs;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esperio.kafka.EsperIOKafkaConfig;
import com.espertech.esperio.kafka.EsperIOKafkaInputAdapterPlugin;
import com.espertech.esperio.kafka.EsperIOKafkaOutputAdapterPlugin;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Properties;

import static com.ur.ifs.RestfulController.STATEMENT_COLLECTION_NAME;

@Component
public class EventProcessor {

    final static String KAFKA_SERVER = "192.168.2.176:9092";
    private final static String GROUP_ID = "EsperConsumer";
    private static final Logger LOG = LoggerFactory.getLogger(EventProcessor.class);
    private Properties kafkaInputProps = new Properties();
    private Properties kafkaOutputProps = new Properties();
    private Configuration configuration = new Configuration();
    private EPRuntime runtime;
    @Autowired
    private MongoDatabase mongoDatabase;

    @PostConstruct
    public void postConstruct() {

        //Consumer Configuration
        kafkaInputProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        kafkaInputProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        kafkaInputProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        kafkaInputProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        kafkaInputProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        kafkaInputProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Kafka Input Adapter Configuration
        kafkaInputProps.put(EsperIOKafkaConfig.INPUT_SUBSCRIBER_CONFIG, EsperIOKafkaInputSubscriberCustom.class.getName());
        kafkaInputProps.put(EsperIOKafkaConfig.INPUT_PROCESSOR_CONFIG, EsperIOKafkaInputProcessorJsonCustom.class.getName());
        //Only set this one if internal Timer is disabled in Runtime Configuration
        kafkaInputProps.put(EsperIOKafkaConfig.INPUT_TIMESTAMPEXTRACTOR_CONFIG, EsperIOKafkaInputTimestampExtractorCustom.class.getName());

        //Producer Configuration
        kafkaOutputProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        kafkaOutputProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        kafkaOutputProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        //Kafka Output Adapter Configuration
        kafkaOutputProps.put(EsperIOKafkaConfig.OUTPUT_FLOWCONTROLLER_CONFIG, EsperIOKafkaOutputFlowControllerCustom.class.getName());

        //Compiler Configuration
        //Types shall be available for "sendEvent" use
        configuration.getCompiler().getByteCode().setAccessModifiersPublic();
        configuration.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);

        //Runtime Configuration
        configuration.getCommon().addImport(KafkaOutput.class);
        //Internal Timer disabled for Evaluation, external Time is used from custom Timestamp Extractor
        configuration.getRuntime().getThreading().setInternalTimerEnabled(false);
        configuration.getRuntime().addPluginLoader(EsperIOKafkaInputAdapterPlugin.class.getSimpleName(), EsperIOKafkaInputAdapterPlugin.class.getName(), kafkaInputProps, null);
        configuration.getRuntime().addPluginLoader(EsperIOKafkaOutputAdapterPlugin.class.getSimpleName(), EsperIOKafkaOutputAdapterPlugin.class.getName(), kafkaOutputProps, null);

        //Create Runtime
        runtime = EPRuntimeProvider.getRuntime(this.getClass().getSimpleName(), configuration);

        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("deploymentMode", "prod");

        //Deploy Statements from Database into Runtime
        MongoCursor<Document> cursor = mongoDatabase.getCollection(STATEMENT_COLLECTION_NAME).find(whereQuery).iterator();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            String statement = document.get("eplStatement").toString();
            try {
                EPDeployment epd = CompileDeployUnit.compileDeploy(runtime, statement);
                String newDeploymentId = epd.getDeploymentId();
                document.replace("deploymentId", newDeploymentId);
                Document findByObjectId = new Document().append("_id", document.get("_id"));
                mongoDatabase.getCollection(STATEMENT_COLLECTION_NAME).findOneAndReplace(findByObjectId, document);
                LOG.info("Statement from Database with ObjectId: " + document.get("_id") + " successfully deployed into runtime with DeploymentId: " + epd.getDeploymentId());

            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }

        //Add dependencies to Statements in Database
        MongoCursor<Document> cursor2 = mongoDatabase.getCollection(STATEMENT_COLLECTION_NAME).find(whereQuery).iterator();
        while (cursor2.hasNext()) {
            Document document = cursor2.next();
            String deploymentId = document.get("deploymentId").toString();
            EPDeployment epd = runtime.getDeploymentService().getDeployment(deploymentId);
            String[] dependencies = epd.getDeploymentIdDependencies();
            document.put("deploymentDependencies", Arrays.asList(dependencies));
            Document findByObjectId = new Document().append("_id", document.get("_id"));
            mongoDatabase.getCollection(STATEMENT_COLLECTION_NAME).findOneAndReplace(findByObjectId, document);
        }
        LOG.info("Updated deploymentDependencies for all Statements in Database");
    }

    EPRuntime getRuntime() {
        return runtime;
    }
}