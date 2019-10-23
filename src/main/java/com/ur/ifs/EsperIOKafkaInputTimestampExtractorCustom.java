package com.ur.ifs;

import com.espertech.esperio.kafka.EsperIOKafkaInputTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/*Extrahiert den Timestamp aus den JSON-Events.
  Der Key in den JSON-Events muss timestamp heißen und vom Datentyp long sein*/
public class EsperIOKafkaInputTimestampExtractorCustom implements EsperIOKafkaInputTimestampExtractor {

    private JSONParser parser = new JSONParser();

    public EsperIOKafkaInputTimestampExtractorCustom() {
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> record) {

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(record.value().toString());
            try {
                long timestamp = (long) jsonObject.get("timestamp");
                return timestamp;
            } catch (NullPointerException npe) {
                return 0;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
