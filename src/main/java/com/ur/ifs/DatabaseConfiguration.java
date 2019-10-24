package com.ur.ifs;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*Klasse wurde aus dem Projektseminar übernommen, stellt eine Verbindung zur MongoDB her*/
@Configuration
public class DatabaseConfiguration {
    private final String databaseName = "VisualCEP";
    private final String host = (System.getenv("MONGO_DB_HOST") != null) ? System.getenv("MONGO_DB_HOST") : "pcrw00159.uni-regensburg.de";
    private final int port = (System.getenv("MONGO_DB_PORT") != null) ? Integer.parseInt(System.getenv("MONGO_DB_PORT")) : 27017;
    private final String username = (System.getenv("MONGO_DB_USERNAME") != null) ? System.getenv("MONGO_DB_USERNAME") : "ifs";
    private final String username = (System.getenv("MONGO_DB_PASSWORD") != null) ? System.getenv("MONGO_DB_PASSWORD") : "changeME0815";

    @Bean
    public MongoDatabase createMongoClient() {
        System.out.println(String.format(": CREATE MONGO CLIENT HOST: %s - PORT: %s", host, Integer.toString(port)));

        MongoClient mc = null;
        ServerAddress serverAddress = new ServerAddress(host, port);
        MongoCredential credential = MongoCredential.createCredential(username, "admin", password.toCharArray());
        MongoClientOptions clientOptions = MongoClientOptions.builder().build();
        mc = new MongoClient(serverAddress, credential, clientOptions);

        MongoDatabase md = mc.getDatabase(databaseName);
        return md;
    }
}