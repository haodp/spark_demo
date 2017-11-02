package com.lakala.spark.service;

import com.alibaba.fastjson.JSON;
import com.lakala.spark.Constants.Constants;
import com.lakala.spark.model.CategorySortKey;
import com.lakala.spark.model.Tmerinfo;
import com.lakala.spark.model.Torder;
import com.lakala.spark.util.DateUtils;
import com.lakala.spark.util.PropertiseUtil;
import com.lakala.spark.util.StringUtils;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by user on 2017/10/30.
 */
@Service
public class DayEndCountService implements java.io.Serializable {

    /**
     * 当天截至信息
     */
    public String countDay() {

        String localName = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.LOCAL_NAME);

        // 1. 创建SparkConf配置信息
        SparkConf conf = new SparkConf()
                .setMaster(localName)
                .setAppName("spark-dayEndCount");

        // 2. 创建SparkContext对象，在java编程中，该对象叫做JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 从hdfs读取文件形成RDD
        // 文件路径自行给定
        String path = PropertiseUtil.getString(Constants.FILE_SPARK, Constants.FILE_PATH);
        JavaRDD<String> rdd = sc.textFile(path + File.separator + "TMERINFO" + File.separator + "*" + File.separator + "part-*");

        // 4.1 行数据的分割，调用flatMap函数
        JavaRDD<Tmerinfo> tmerinfo_records = rdd.map(new Function<String, Tmerinfo>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tmerinfo call(String line) throws Exception {
                if(line == null || line.length() == 0){
                    return null;
                }
                if(line.startsWith(Constants.ZHONGKUHAO)){
                    line = line.substring(1, line.length() - 1);
                }
                String[] fields = line.split(Constants.COMMA);
                if(fields == null || fields.length < 3 ) {
                    return null;
                }
                Tmerinfo sd = new Tmerinfo(fields[0], fields[2]);
                return sd;
            }
        });
        System.out.println("tmerinfo表的记录数:" + tmerinfo_records.collect().size());

        // 商户信息
        JavaPairRDD<String, Tmerinfo> merinfoRDD = getTaccountid2ActionRDD(tmerinfo_records);

        JavaRDD<String> tOrderRDD = sc.textFile(path + File.separator + "TORDER" + File.separator + DateUtils.getYMD() + File.separator  + "part-*");
        // 4.1 行数据的分割，调用flatMap函数
        JavaRDD<Torder> tOrder_records = tOrderRDD.map(new Function<String, Torder>() {
            @Override
            public Torder call(String line) throws Exception {
                if(line == null || line.length() == 0){
                    return null;
                }
                if(line.startsWith(Constants.ZHONGKUHAO)){
                    line = line.substring(1, line.length() - 1);
                }
                String[] fields = line.split(Constants.COMMA);
                if(fields.length < 137 ){
                    return null;
                }

                Torder sd = new Torder(fields[3], fields[137], fields[26]);

                if(Constants.STATE_1.equals(sd.getState())){
                    return sd;
                } else {
                    return null;
                }
            }
        });

        // null记录过滤
        JavaRDD<Torder> tOrderRecords = tOrder_records.filter(new Function<Torder, Boolean>() {
            @Override
            public Boolean call(Torder torder) throws Exception {
                if(torder == null || torder.getStoreid() == null){
                    return false;
                }
                return true;
            }
        });
        // 记录数目
        System.out.println("torder表的记录数:" + tOrder_records.collect().size());

        //聚合
        //首先，可以将行为数据按照 商户号 进行groupByKey分组
        JavaPairRDD<String, String> storeId2AggrInfoRDD = aggregateByStoreId(sc, tOrderRecords, merinfoRDD);

        // 获取top20 交易量的数据
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(storeId2AggrInfoRDD);


        List<HashMap<String, Object>> listResult = new ArrayList<HashMap<String, Object>>();
        for(Tuple2<CategorySortKey, String> item : top10CategoryList){
            HashMap<String, Object> map = new HashMap<String, Object>();
            String line = item._2();

            BigDecimal allmoney = new BigDecimal(StringUtils.getFieldFromConcatString(
                    line, "\\|", Constants.PARAM_ALL_MONEY));
            int cnt = Integer.valueOf(StringUtils.getFieldFromConcatString(
                    line, "\\|", Constants.PARAM_CNT));
            String storeId = StringUtils.getFieldFromConcatString(
                    line, "\\|", Constants.PARAM_STOREID);
            String name = StringUtils.getFieldFromConcatString(
                    line, "\\|", Constants.PARAM_MERNAME);

            map.put(Constants.PARAM_ALL_MONEY, allmoney);
            map.put(Constants.PARAM_CNT, cnt);
            map.put(Constants.PARAM_STOREID, storeId);
            map.put(Constants.PARAM_MERNAME, name);
            map.put(Constants.PARAM_FEE, allmoney.divide(new BigDecimal(120)));
            listResult.add(map);
        }
        sc.stop();

        String result = JSON.toJSONString(listResult);
        return result;
    }


    /**
     * 获取Top10件数目
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(
            JavaPairRDD<String, String> storeId2AggrInfoRDD) {

        /**
         * 第1步：将数据映射成<SortKey,info>格式的RDD，然后进行二次排序
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = storeId2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, CategorySortKey, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<CategorySortKey, String> call(
                            Tuple2<String, String> tuple) throws Exception {
                        String countInfo = tuple._2;
                        BigDecimal allmoney = new BigDecimal(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", "allmoney"));
                        int cnt = Integer.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", "cnt"));

                        CategorySortKey sortKey = new CategorySortKey(allmoney, cnt);
                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }

                });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        /**
         * 第2步：用kake(10)取出top20
         */
        Long num = PropertiseUtil.getLong(Constants.FILE_SPARK, Constants.TOP_NUM);
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(num.intValue());
        return top10CategoryList;
    }


    private static JavaPairRDD<String, String> aggregateByStoreId(
            JavaSparkContext context, JavaRDD<Torder> actionRDD, JavaPairRDD<String, Tmerinfo> merinfoRDD) {

        System.out.println(actionRDD.collect());

        //现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        //现在需要将这个Row映射成<storeid,Row>的格式
        JavaPairRDD<String, Torder> storeidStoreIdRDD = actionRDD.mapToPair(
                /**
                 * PairFunction
                 * 第一个参数，相当于是函数的输入
                 * 第二个参数和第三个参数，相当于是函数的输出（Tuple），分别是Tuple第一个和第二个值
                 */
                new PairFunction<Torder, String, Torder>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Torder> call(Torder row) throws Exception {
                        //此时需要拿到store_id
                        return new Tuple2<String, Torder>(row.getStoreid(), row);
                    }
                });

        //对行为数据按照 商户 粒度进行分组
        JavaPairRDD<String, Iterable<Torder>> storeid2ActionsRDD = storeidStoreIdRDD.groupByKey();

        //对每一个storeid分组进行聚合，将storeid中所有的数值计算
        JavaPairRDD<String, String> storeId2PartAggrInfoRDD = storeid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Torder>>, String, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Iterable<Torder>> tuple)
                            throws Exception {
                        String storeId = tuple._1;
                        Iterator<Torder> iterator = tuple._2.iterator();

                        // 返回结果
                        StringBuffer resultBuffer = new StringBuffer("");
                        // 交易笔数
                        int cnt = 0;
                        BigDecimal allMoney = new BigDecimal(0);
                        //遍历所有的行为
                        while(iterator.hasNext()) {
                            cnt++;
                            Torder row = iterator.next();
                            allMoney = allMoney.add(new BigDecimal(row.getPayamount()));
                        }

                        //在Constants.java中定义spark作业相关的常量
                        // 做成返回字符串
                        resultBuffer.append("storeid=");
                        resultBuffer.append(storeId);
                        resultBuffer.append("|");
                        resultBuffer.append("allmoney=");
                        resultBuffer.append(allMoney);
                        resultBuffer.append("|");
                        resultBuffer.append("cnt=");
                        resultBuffer.append(cnt);

                        return new Tuple2<String, String>(storeId, resultBuffer.toString());
                    }

                });

        System.out.println("storeId2PartAggrInfoRDD：-----" + storeId2PartAggrInfoRDD.collect().size());

        //将storeid粒度聚合数据，与用户信息进行join
        JavaPairRDD<String, Tuple2<String, Tmerinfo>> storeid2FullInfoRDD = storeId2PartAggrInfoRDD.join(merinfoRDD);

        //对join起来的数据进行拼接，并且返回<storeid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> storeid2FullAggrInfoRDD = storeid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Tmerinfo>>, String, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<String, Tuple2<String, Tmerinfo>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Tmerinfo row = tuple._2._2;

                        String storeId = row.getTaccountid();

                        //在Constants.java中添加以下常量
                        String fullAggrInfo = partAggrInfo + "|"
                                + "mername=" + row.getMername();
                        return new Tuple2<String, String>(storeId, fullAggrInfo);
                    }
                });
        return storeid2FullAggrInfoRDD;
    }


    /**
     * 获取商户号到访问行为数据的映射的RDD
     * @param actionRDD
     * @return
     */
    public JavaPairRDD<String, Tmerinfo> getTaccountid2ActionRDD(JavaRDD<Tmerinfo> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Tmerinfo, String, Tmerinfo>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Tmerinfo> call(Tmerinfo row) throws Exception {
                return new Tuple2<String, Tmerinfo>(row.getTaccountid(), row);
            }
        });
    }
}
