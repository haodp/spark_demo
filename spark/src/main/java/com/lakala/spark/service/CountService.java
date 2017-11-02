package com.lakala.spark.service;

import com.alibaba.fastjson.JSON;
import com.lakala.spark.Constants.Constants;
import com.lakala.spark.model.Ttransaction;
import com.lakala.spark.model.TtransactionResult;
import com.lakala.spark.util.PropertiseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.File;
import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.Iterator;


/**
 * Created by user on 2017/10/30.
 */
@Service
public class CountService implements java.io.Serializable {

    /**
     * 当天截至信息
     * @param code 1:总结交易数目 2:参数
     */
    public String count(String code) {

        String localName = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.LOCAL_NAME);

        // 1. 创建SparkConf配置信息
        SparkConf conf = new SparkConf()
                .setMaster(localName)
                .setAppName("spark-allCount");

        // 2. 创建SparkContext对象，在java编程中，该对象叫做JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 从hdfs读取文件形成RDD
        // 文件路径自行给定
        String path = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.FILE_PATH);


        JavaRDD<String> rdd = sc.textFile(path + File.separator + "TTRANSACTION" + File.separator + "*" + File.separator + "part-*");

        long test = rdd.count();
        // 4.1 行数据的分割，调用flatMap函数
        JavaRDD<Ttransaction> transaction_records = rdd.map(new Function<String, Ttransaction>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Ttransaction call(String line) throws Exception {
                if(line == null || line.length() == 0){
                    return null;
                }
                if(line.startsWith(Constants.ZHONGKUHAO)){
                    line = line.substring(1, line.length() - 1);
                }
                String[] fields = line.split(Constants.COMMA);
                if(fields == null || fields.length < 11 ) {
                    return null;
                }
                Ttransaction sd = new Ttransaction(fields[11], fields[3], fields[4], 1);
                return sd;
            }
        });
        System.out.println("Ttransaction表的记录数:" + transaction_records.collect().size());

        //聚合
        //首先，可以将行为数据按照 商户号 进行groupByKey分组
        JavaPairRDD<String, Ttransaction> storeId2AggrInfoRDD = aggregateByStoreId(sc, transaction_records);

        TtransactionResult listResult = new TtransactionResult();

        BigDecimal money = new BigDecimal(0);
        BigDecimal fee = new BigDecimal(0);
        int cnt = 0;

        int failCnt = 0;
        for(Tuple2<String, Ttransaction> item : storeId2AggrInfoRDD.collect()){
            // 状态值
            String state =  item._1();
            Ttransaction line = item._2();

            money = money.add(new BigDecimal(line.getAmt()));
            fee = fee.add(new BigDecimal(line.getFee()));
            cnt = cnt + line.getCnt();

            if("2".equals(state)){
                failCnt = failCnt + line.getCnt();
            }
        }
        sc.stop();

        listResult.setAmt(String.valueOf(money));
        listResult.setFee(String.valueOf(fee));
        listResult.setAmtCnt(String.valueOf(cnt));
        listResult.setFailCnt(String.valueOf(failCnt));
        NumberFormat nt = NumberFormat.getPercentInstance();
        //设置百分数精确度2即保留两位小数
        nt.setMinimumFractionDigits(2);
        listResult.setFailPer(nt.format(failCnt/cnt));

        String result = JSON.toJSONString(listResult);
        return result;
    }


    private static JavaPairRDD<String, Ttransaction> aggregateByStoreId(
            JavaSparkContext context, JavaRDD<Ttransaction> actionRDD) {

        //现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        //现在需要将这个Row映射成<storeid,Row>的格式
        JavaPairRDD<String, Ttransaction> storeidStoreIdRDD = actionRDD.mapToPair(
                /**
                 * PairFunction
                 * 第一个参数，相当于是函数的输入
                 * 第二个参数和第三个参数，相当于是函数的输出（Tuple），分别是Tuple第一个和第二个值
                 */
                new PairFunction<Ttransaction, String, Ttransaction>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Ttransaction> call(Ttransaction row) throws Exception {
                        //此时需要拿到store_id
                        return new Tuple2<String, Ttransaction>(row.getState(), row);
                    }
                });

        //对行为数据按照 商户 粒度进行分组
        JavaPairRDD<String, Iterable<Ttransaction>> storeid2ActionsRDD = storeidStoreIdRDD.groupByKey();

        //对每一个storeid分组进行聚合，将storeid中所有的数值计算
        JavaPairRDD<String, Ttransaction> storeId2PartAggrInfoRDD = storeid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Ttransaction>>, String, Ttransaction>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Ttransaction> call(Tuple2<String, Iterable<Ttransaction>> tuple)
                            throws Exception {
                        String state = tuple._1;
                        Iterator<Ttransaction> iterator = tuple._2.iterator();

                        // 返回结果
                        Ttransaction resultBuffer = new Ttransaction();
                        // 交易笔数
                        int cnt = 0;
                        // 交易笔数
                        BigDecimal money = new BigDecimal(0);
                        BigDecimal fee = new BigDecimal(0);
                        //遍历所有的行为
                        while(iterator.hasNext()) {
                            cnt++;
                            Ttransaction row = iterator.next();
                            money = money.add(new BigDecimal(row.getAmt()));
                            fee = fee.add(new BigDecimal(row.getFee()));
                        }

                        //在Constants.java中定义spark作业相关的常量
                        // 做成返回字符串
                        resultBuffer.setAmt(String.valueOf(money));
                        resultBuffer.setFee(String.valueOf(fee));
                        resultBuffer.setCnt(cnt);
                        resultBuffer.setState(state);

                        return new Tuple2<String, Ttransaction>(state, resultBuffer);
                    }

                });

        System.out.println("storeId2PartAggrInfoRDD：-----" + storeId2PartAggrInfoRDD.collect().size());

        return storeId2PartAggrInfoRDD;
    }


}
