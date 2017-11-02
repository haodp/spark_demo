package com.lakala.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Created by user on 2017/10/30.
 */
@SpringBootApplication
@ComponentScan
@EnableAutoConfiguration
@EnableScheduling
public class SparkApp {

    public static void main(String[] args) {
        SpringApplication.run(SparkApp.class, args);
    }

}
