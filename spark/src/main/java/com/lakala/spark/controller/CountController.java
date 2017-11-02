package com.lakala.spark.controller;

import com.alibaba.fastjson.JSON;
import com.lakala.spark.service.CountService;
import com.lakala.spark.service.DayEndCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * Created by user on 2017/10/30.
 */
@RestController
public class CountController {

    @Autowired
    CountService service;

    @RequestMapping("/count")
    public String countDay(@PathVariable String code) {
        String result = "";
        HashMap<String,String> map = new HashMap<String, String>();
        try{
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
        CountService service = new CountService();
        String code = "1";
        String result = service.count(code);
        System.out.println(result);
    }
}
