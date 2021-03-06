package com.lakala.spark.service;

import com.lakala.spark.Constants.Constants;
import com.lakala.spark.util.DateUtils;
import com.lakala.spark.util.FileUtils;
import com.lakala.spark.util.PropertiseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

/**
 * Created by user on 2017/10/30.
 */
public class OracleToFileService implements java.io.Serializable {

    public void selectOracleToFile() throws IOException {
        String resultHDFSSavePath = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.FILE_PATH);

        String localName = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.LOCAL_NAME);

        SparkConf conf = new SparkConf().setAppName("readOracle").setMaster(localName) ;
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String url = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.URL);
        String driver = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.DRIVER);
        String user = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.USER);
        String password = PropertiseUtil.getString(Constants.FILE_JDBC, Constants.PASSWORD);

        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url", url);
        reader.option("dbtable", "TMERINFO");
        reader.option("driver", driver);
        reader.option("user", user);
        reader.option("password", password);

        Dataset<Row> tmerinfoDF = reader.load();
        JavaRDD<Row> javaRDD = tmerinfoDF.javaRDD();
        String filePath = resultHDFSSavePath + File.separator + "TMERINFO";
        // 文件夹检查
        if(localName.toLowerCase().startsWith("spark")){
            Configuration hadoopConf = sc.hadoopConfiguration();
            FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf);
            Path fdfsfilePath = new Path(filePath);
            if(hdfs.exists(fdfsfilePath)){
                //为防止误删，禁止递归删除
                hdfs.delete(fdfsfilePath,true);
            }
        } else {
            if(FileUtils.checkDir(filePath)){
                // 清空文件夹
                FileUtils.deleteDir(new File(filePath));
            }
        }
        javaRDD.saveAsTextFile(filePath + File.separator + DateUtils.getYMD());

        // 其它表的读取
        String tables = PropertiseUtil.getString(Constants.FILE_SPARK, "tables.list");

        if(tables == null || tables.length() == 0){
            sc.stop();
            return;
        }
        String[] tablesList = tables.split(Constants.COMMA);

        for(String item : tablesList){
            String itemPath = resultHDFSSavePath + File.separator + item;

            String colFilter = PropertiseUtil.getString(Constants.FILE_SPARK, item + ".filter");
            boolean flag = false;
            if(localName.toLowerCase().startsWith("spark")){
                Configuration hadoopConf = sc.hadoopConfiguration();
                FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf);
                Path fdfsfilePath = new Path(itemPath);
                flag = hdfs.exists(fdfsfilePath);
            } else {
                flag = FileUtils.checkDir(itemPath);
            }
            // 文件夹检查
            if(!flag){
                // 全部文件的做成
                Calendar startDate = DateUtils.startDate2015();

                reader.option("dbtable", item);
                tmerinfoDF = reader.load();
                while (DateUtils.compareSysdate(startDate)){
                    String exeDate = DateUtils.formatYMD(startDate.getTime());
                    javaRDD = tmerinfoDF.filter(functions.col(colFilter).startsWith(exeDate)).javaRDD();
                    if(javaRDD.collect() != null && javaRDD.collect().size() > 0){
                        // 文件保存
                        javaRDD.saveAsTextFile(itemPath + File.separator + exeDate);
                    }
                    // 日期添加１
                    startDate = DateUtils.addDate(startDate, 1);
                }
            } else {
                // 当天文件的清空。
                itemPath = itemPath + File.separator + DateUtils.getYMD();
                if(localName.toLowerCase().startsWith("spark")) {
                    Configuration hadoopConf = sc.hadoopConfiguration();
                    FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf);
                    Path fdfsfilePath = new Path(itemPath);
                    if (hdfs.exists(fdfsfilePath)) {
                        //为防止误删，禁止递归删除
                        hdfs.delete(fdfsfilePath, true);
                    }
                } else {
                    if(FileUtils.checkDir(itemPath)){
                        FileUtils.deleteDir(new File(itemPath));
                    }
                }
                reader.option("dbtable", item);
                tmerinfoDF = reader.load();
                String exeDate = DateUtils.getYMD();
                javaRDD = tmerinfoDF.filter(functions.col(colFilter).startsWith(exeDate)).javaRDD();
                // 文件保存
                javaRDD.saveAsTextFile(itemPath);
            }
        }
        sc.stop();
    }

}
