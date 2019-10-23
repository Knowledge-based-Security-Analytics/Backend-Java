package com.ur.ifs;


import com.espertech.esper.common.client.json.minimaljson.JsonObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MongoDBImportCSV {

    static final String inputCSVName = "august_week1_udp_scan.csv";
    private static final String inputCSVPath = "D:\\";
    private static final String[] keys = "timestamp, duration, srcIP, dstIP, srcPort, dstPort, protocol, flags, fwd, tos, packets, bytes".split(", ");
    @Autowired
    private MongoDatabase mongoDatabase;

    /*Importiert ein UGR 16 CSV File in Form von JSON-Events in die MongoDB.
    Zur Performanceoptimierung kann im Nachgang das Datum in der MongoDB indiziert werden*/
    @Test
    public void importCSV() throws IOException {

        MongoCollection<Document> mc = mongoDatabase.getCollection(inputCSVName);
        File inputCSV = new File(inputCSVPath + inputCSVName);
        InputStream inputCSVStream = new FileInputStream(inputCSV);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputCSVStream));
        long inserted = 0;
        long read = 0;
        boolean bool = true;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        br.readLine();
        br.readLine();
        String[] line = br.readLine().split(",");
        //int k= 0;
        while (bool) {
            try {
                JsonObject jsonObject = new JsonObject();
                for (int i = 0; i < keys.length; i++) {
                    if (i == 1) {
                        jsonObject.add(keys[i], Float.parseFloat(line[i]));
                    } else if (i == 4 || i == 5 || i == 8 || i == 9 || i == 10 || i == 11) {
                        try {
                            jsonObject.add(keys[i], Integer.parseInt(line[i]));
                        } catch (NumberFormatException nfe) {
                            jsonObject.add(keys[i], Long.parseLong(line[i]));
                        }
                    } else {
                        jsonObject.add(keys[i], line[i]);
                    }
                }

                Document document = Document.parse(jsonObject.toString());
                Date date = format.parse(document.getString("timestamp"));
                document.replace("timestamp", date);
                mc.insertOne(document);
                inserted++;
                read++;
            } catch (Exception e) {
                read++;
                System.out.println("Drecksline mit Nummer: " + read + " " + Arrays.toString(line));
                e.printStackTrace();
            }
            try {
                line = br.readLine().split(",");
            } catch (NullPointerException nep) {
                System.out.println("End of file reached!");
                System.out.println("Read " + read + " lines into Database!");
                System.out.println("Imported " + inserted + " lines into Database!");
                br.close();
                inputCSVStream.close();
                nep.printStackTrace();
                bool = false;
            }
        }
    }
}
