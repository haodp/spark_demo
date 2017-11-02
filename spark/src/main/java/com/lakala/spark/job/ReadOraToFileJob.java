package com.lakala.spark.job;

import com.lakala.spark.service.OracleToFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by user on 2017/10/30.
 */
@Component
public class ReadOraToFileJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        OracleToFileService service = new OracleToFileService();
        service.selectOracleToFile();
    }

    @Autowired
    OracleToFileService service;
    @Scheduled(cron = "10 0 0/1 * * ?")
    public void makeOracleFile() {
        logger.info("oracle file job is starting");
        service.selectOracleToFile();
        logger.info("oracle file job is ending");
    }

}
