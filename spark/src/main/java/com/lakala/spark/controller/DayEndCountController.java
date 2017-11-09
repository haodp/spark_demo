package com.lakala.spark.controller;

import com.alibaba.fastjson.JSON;
import com.lakala.spark.service.CountService;
import com.lakala.spark.service.DayEndCountService;

import java.util.HashMap;

import static spark.Spark.get;

/**
 * Created by user on 2017/10/30.
 */
public class DayEndCountController {

    public static String countDay() {
        String result = "";
        HashMap<String,String> map = new HashMap<String, String>();
        try{
            DayEndCountService service = new DayEndCountService();
            result = service.countDay();
            map.put("isSuccess", "true");
            map.put("msg", "处理成功");
            map.put("context", result);
        } catch (Exception ex){
            map.put("isSuccess", "false");
            ex.printStackTrace();
            map.put("msg", "处理失败,请联系管理员." + ex.getMessage());
            map.put("context", null);
        }

        return JSON.toJSONString(map);

    }


    public static String countDayAll(String code) {
        String result = "";
        HashMap<String,String> map = new HashMap<String, String>();
        try{
            CountService service = new CountService();
            result = service.count(code);
            map.put("isSuccess", "true");
            map.put("msg", "处理成功");
            map.put("context", result);
        } catch (Exception ex){
            map.put("isSuccess", "false");
            map.put("msg", "处理失败,请联系管理员.");
            map.put("context", null);
        }
        return JSON.toJSONString(map);
    }

    public static void main(String[] args) {
        get("/countDay", (req, res) -> countDay());
        String code = "1";
        get("/countAll", (req, res) -> countDayAll(code));
    }
}
