package com.master.controller;

import com.master.service.QuService;
import com.model.config.SysConfigBean;
import com.model.input.CoreQuery;
import com.model.input.QueryRequest;
import com.service.base.AbstractTraceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
public class InternalController {

    @Autowired
    private AbstractTraceService traceService;

    @Autowired
    private QuService quService;

    @RequestMapping(value = {"/", "/health"}, method = RequestMethod.GET)
    public ResponseEntity<Integer> healthCheck() {
        return ResponseEntity.ok(200);
    }

    @RequestMapping(value = {"config"}, method = RequestMethod.GET)
    public ResponseEntity<Map<String, Object>> loadSysConfig(boolean defaultConfig) {
        return ResponseEntity.ok(traceService.loadSysConfig(defaultConfig));
    }

    @RequestMapping(value = {"config"}, method = RequestMethod.POST)
    public ResponseEntity<String> updConfig(@RequestBody SysConfigBean configItem) {
        return ResponseEntity.ok(traceService.updSysConfig(configItem));
    }

    @RequestMapping(value = {"config"}, method = RequestMethod.DELETE)
    public ResponseEntity<String> revertConfig(String operator) {
        return ResponseEntity.ok(traceService.revertConfig(operator));
    }

    @RequestMapping(value = "recognize", method = RequestMethod.POST)
    public ResponseEntity<QueryRequest> recognize(@RequestBody CoreQuery query) {
        return ResponseEntity.ok(quService.handle(query));
    }
}
