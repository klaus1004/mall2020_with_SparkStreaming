package com.klaus.mall2020.mall2020logger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;


    @RequestMapping("/applog")
    public String applog(@RequestBody JSONObject jsonObject){
        String logJson = jsonObject.toJSONString();
        log.info(logJson);
        if(jsonObject.getString("start")!=null){
            kafkaTemplate.send("GMALL_START",logJson);
        }else{
            kafkaTemplate.send("GMALL_EVENT",logJson);

        }

        return "success";
    }


}
