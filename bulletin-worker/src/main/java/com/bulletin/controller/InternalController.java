package com.bulletin.controller;

import com.model.config.SysConfigBean;
import com.service.TraceServiceInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
public class InternalController {

    @Autowired
    private TraceServiceInterface traceService;

    @GetMapping(value = {"/", "/health"})
    public ResponseEntity<Integer> healthCheck() {
        return ResponseEntity.ok(200);
    }

    @GetMapping(value = {"config"})
    public ResponseEntity<Map<String, Object>> loadSysConfig(boolean defaultConfig) {
        return ResponseEntity.ok(traceService.loadSysConfig(defaultConfig));
    }

    @PostMapping(value = {"config"})
    public ResponseEntity<String> updConfig(@RequestBody SysConfigBean configItem) {
        return ResponseEntity.ok(traceService.updSysConfig(configItem));
    }

    @DeleteMapping(value = {"config"})
    public ResponseEntity<String> revertConfig(String operator) {
        return ResponseEntity.ok(traceService.revertConfig(operator));
    }
}
