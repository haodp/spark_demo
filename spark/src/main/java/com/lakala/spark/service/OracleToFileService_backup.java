package com.lakala.spark.service;

/**
 * Created by user on 2017/10/30.
 */
public class OracleToFileService_backup implements java.io.Serializable {

    public void selectOracleToFile() {
//        String resultHDFSSavePath = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.FILE_PATH);
//
//        String localName = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.LOCAL_NAME);
//
//        SparkConf conf = new SparkConf().setAppName("readOracle").setMaster(localName) ;
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc);
//
//        String url = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.URL);
//        String driver = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.DRIVER);
//        String user = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.USER);
//        String password = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.PASSWORD);
//
//        DataFrameReader reader = sqlContext.read().format("jdbc");
//        reader.option("url", url);
//        reader.option("dbtable", "TMERINFO");
//        reader.option("driver", driver);
//        reader.option("user", user);
//        reader.option("password", password);
//
//        Dataset<Row> tmerinfoDF = reader.load();
//        JavaRDD<Row> javaRDD = tmerinfoDF.javaRDD();
//        String filePath = resultHDFSSavePath + File.separator + "TMERINFO";
//        // 文件夹检查
//        if(FileUtils.checkDir(filePath)){
//            // 清空文件夹
//            FileUtils.deleteDir(new File(filePath));
//        }
//        javaRDD.saveAsTextFile(filePath + File.separator + DateUtils.getYMD());
//
//        // 其它表的读取
//        String tables = PropertiseUtil.getString(Constants.FILE_SPARK, "tables.list");
//
//        if(tables == null || tables.length() == 0){
//            sc.stop();
//            return;
//        }
//        String[] tablesList = tables.split(Constants.COMMA);
//
//        for(String item : tablesList){
//            String itemPath = resultHDFSSavePath + File.separator + item;
//
//            Long num = PropertiseUtil.getLong(Constants.FILE_SPARK, item + ".filter");
//            // TODO 文件夹检查
//            if(false){
//                // 全部文件的做成
//                Calendar startDate = DateUtils.startDate2015();
//                int days = 1;
//                while (DateUtils.compareSysdate(startDate)){
//
//                    String strDate = DateUtils.formatYMD(startDate.getTime());
//                    reader.option("dbtable", item);
//                    javaRDD = tmerinfoDF.javaRDD();
//
//                    if(javaRDD.collect() != null && javaRDD.collect().size() > 0){
//                        // 文件保存
//                        javaRDD.saveAsTextFile(itemPath + File.separator + strDate);
//                    }
//                    // 日期添加１
//                    startDate = DateUtils.addDate(startDate, days);
//                    days++;
//                }
//            } else {
//                // 当天文件的清空。
//                itemPath = itemPath + File.separator + DateUtils.getYMD();
//                if(FileUtils.checkDir(itemPath)){
//                    FileUtils.deleteDir(new File(itemPath));
//                }
//                reader.option("dbtable", item);
//                tmerinfoDF = reader.load();
//                Timestamp date = DateUtils.getSysTimeStart();
////                Timestamp.valueOf();
//                javaRDD = tmerinfoDF.filter(functions.col("PLATTIME").gt("2017-11-01")).javaRDD();
//
////                javaRDD = tmerinfoDF.javaRDD();
//                // 文件保存
//                javaRDD.saveAsTextFile(itemPath);
//            }
//        }
//        sc.stop();
    }

}
