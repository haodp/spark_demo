package com.lakala.spark.job;

import com.lakala.spark.service.OracleToFileService;

/**
 * Created by user on 2017/10/30.
 */
public class ReadOraToFileJob {


    public static void main(String[] args) {
        OracleToFileService service = new OracleToFileService();
        try{
            service.selectOracleToFile();
        } catch (Exception ex){
            ex.printStackTrace();
        }

    }

//    @Autowired
//    OracleToFileService service;
//    @Scheduled(cron = "10 0 0/1 * * ?")
//    public void makeOracleFile() {
//        logger.info("oracle file job is starting");
//        service.selectOracleToFile();
//        logger.info("oracle file job is ending");
//    }

}
