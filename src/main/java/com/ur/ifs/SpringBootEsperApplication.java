package com.ur.ifs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

@SpringBootApplication(scanBasePackages = {"com.ur.ifs"}, exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
public class SpringBootEsperApplication {
    public SpringBootEsperApplication() {}

    public static void main(String[] args) {
        SpringApplication.run(SpringBootEsperApplication.class, args);
    }
}