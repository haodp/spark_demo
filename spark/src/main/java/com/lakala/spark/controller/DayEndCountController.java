package com.lakala.spark.controller;

import com.alibaba.fastjson.JSON;
import com.lakala.spark.service.DayEndCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * Created by user on 2017/10/30.
 */
@RestController
public class DayEndCountController {

    @Autowired
    DayEndCountService service;

    @RequestMapping("/countDay")
    public String countDay() {
        String result = "";
        HashMap<String,String> map = new HashMap<String, String>();
        try{
            result = service.countDay();
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
        DayEndCountService service = new DayEndCountService();
        String result = service.countDay();
        System.out.println(result);
    }
}
