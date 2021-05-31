package com.github.bednar.influxdbreactorrestwriteexample;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringReactorInfluxDbClientWriteExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringReactorInfluxDbClientWriteExampleApplication.class, args);
    }

    @Bean
    public InfluxDBClient influxDBClient() {
        String url = "http://localhost:8086";
        char[] token = "my-token".toCharArray();

        return InfluxDBClientFactory.create(url, token).setLogLevel(LogLevel.BODY);
    }
}
