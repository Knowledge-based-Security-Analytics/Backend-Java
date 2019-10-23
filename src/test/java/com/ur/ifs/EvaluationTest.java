package com.ur.ifs;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import javafx.util.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ur.ifs.EventProcessor.KAFKA_SERVER;
import static com.ur.ifs.MongoDBImportCSV.inputCSVName;
import static com.ur.ifs.RestfulController.STATEMENT_COLLECTION_NAME;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@SuppressWarnings("unchecked call")
public class EvaluationTest {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationTest.class);
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private MongoDatabase mongoDatabase;
    private JSONParser parser = new JSONParser();
    private ArrayList<String> statements = new ArrayList<>();

    /* ACHTUNG, das Ausführen dieses Tests löscht alle Statements aus der Runtime und der Datenbank!
    Siehe Kapitel Evaluation in der Bachelorarbeit für eine Beschreibung des Testablaufs.
    Damit die Evaluation durchgeführt werden kann, muss zuvor der UGR 16 Port PortScan-Angriff in die MongoDB importiert werden. (siehe zweite Testklasse)
    Die Ergebnisse, die in der MongoDB gespeichert werden, können recht schön mit der GUI MongoCompass begutachtet werden.*/
    @Test
    public void portScanEvaluation() throws Exception {
        this.mockMvc.perform(delete("/statement/all"))
                .andExpect(status().isOk());
        Thread.sleep(1000);

        statements.add("@JsonSchema(dynamic=true) create json schema LogEvent(srcIP string, srcPort int, dstIP string, dstPort int);");
        statements.add("create table ScanCountTable(srcIP string primary key, dstIP string primary key, cnt count(*), win window(*) @type(LogEvent));");
        statements.add("into table ScanCountTable insert into CountStream select srcIP, dstIP, count(*) as cnt, window(*) as win from LogEvent#unique(srcIP, dstIP, dstPort)#time(10 min) group by srcIP,dstIP;");
        statements.add("create window SituationsWindow#keepall() (srcIP string, dstIP string, detectionTime long);");
        statements.add("on CountStream(cnt >= 50) as cs merge SituationsWindow sw where cs.srcIP = sw.srcIP and cs.dstIP = sw.dstIP when not matched then insert select srcIP, dstIP, current_timestamp as detectionTime then insert into OutputAlerts select cs.srcIP as srcIP, cs.dstIP as dstIP;");
        statements.add("@KafkaOutput('PortScanEvent') select * from OutputAlerts;");
        statements.add("@JsonSchema(dynamic=true) create json schema PortScanEvent(srcIP string, dstIP string);");
        statements.add("@KafkaOutput('IPBlacklistEvent') select srcIP as IP from PortScanEvent.win:time(10 min) group by srcIP having count(*) > 10 output first every 15 minutes;");
        statements.add("@JsonSchema(dynamic=true) create json schema IPBlacklistEvent (IP string);");
        for (String statement : statements) {
            this.mockMvc.perform(post("/statement")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content("{\"statement\":\"" + statement + "\"}"))
                    .andExpect(status().isOk());
        }
        Thread.sleep(1000);
        MongoCursor<Document> cursor = mongoDatabase.getCollection(STATEMENT_COLLECTION_NAME).find().iterator();
        Assert.assertTrue(cursor.hasNext());

        int i = 0;
        while (cursor.hasNext()) {
            Document document = cursor.next();
            System.out.println(document);
            Assert.assertEquals(document.get("statement"), statements.get(i));
            i++;
        }

        KafkaProducer producer = new KafkaProducer(createKafkaProducerProps());
        KafkaConsumer<String, String> consumer = new KafkaConsumer(createKafkaConsumerProps());
        consumer.subscribe(Arrays.asList("PortScanEvent", "IPBlacklistEvent"));
        Thread.sleep(1000);

        MongoCursor<Document> cursor2 = mongoDatabase.getCollection(inputCSVName).find().iterator();
        Assert.assertTrue(cursor2.hasNext());

        while (cursor2.hasNext()) {
            Document document = cursor2.next();
            document.remove("_id");
            JSONObject jsonObject = (JSONObject) parser.parse(document.toJson());
            JSONObject timestamp = (JSONObject) jsonObject.remove("timestamp");
            jsonObject.put("timestamp", timestamp.get("$date"));
            producer.send(new ProducerRecord<String, String>("LogEvent", jsonObject.toString()));
        }
        Thread.sleep(1000);
        producer.close();

        AtomicInteger consumedPortScanEventCount = new AtomicInteger();
        AtomicInteger consumedIPBlacklistEventCount = new AtomicInteger();
        long startTime = System.currentTimeMillis();

        LOG.info("Wait until 4097 PortScanEvents and 1 IPBlacklistEvent is consumed or fail after 5 minutes");
        Duration dur = new Duration(5000);
        while (true) {

            if (consumedPortScanEventCount.get() > 4097 || consumedIPBlacklistEventCount.get() > 1) {
                Assert.fail("Received more Events then expected");
            }
            if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(5)) {
                Assert.fail("Waited 5 Minutes, expected Events have not been consumed");
            }

            if (consumedPortScanEventCount.get() == 4097 && consumedIPBlacklistEventCount.get() == 1) {
                consumer.commitSync();
                consumer.close();
                break;
            }

            final ConsumerRecords<String, String> consumerRecords = consumer.poll(5000);
            consumerRecords.forEach(record -> {
                mongoDatabase.getCollection(record.topic()).insertOne(Document.parse(record.value()));
                if (record.topic().equals("PortScanEvent")) consumedPortScanEventCount.getAndIncrement();
                if (record.topic().equals("IPBlacklistEvent")) consumedIPBlacklistEventCount.getAndIncrement();
            });

            LOG.info("consumedPortScanEventCount: " + consumedPortScanEventCount);
            LOG.info("consumedIPBlacklistEventCount: " + consumedIPBlacklistEventCount);
            consumer.commitAsync();
        }

        Assert.assertEquals(4097, consumedPortScanEventCount.get());
        Assert.assertEquals(1, consumedIPBlacklistEventCount.get());
    }

    private Properties createKafkaProducerProps() {
        Properties producerProps = new Properties();
        //Producer Configuration
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, EvaluationTest.class.getName() + "_Producer");
        return producerProps;
    }

    private Properties createKafkaConsumerProps() {
        Properties consumerProps = new Properties();
        //Consumer Configuration
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, EvaluationTest.class.getName() + "_ConsumerGroup");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProps;
    }

}
