package com.research.controller;

import com.model.SortItem;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.model.output.Response;
import com.service.WorkerSearchServiceInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping(value = "v1/api")
public class SearchController {

    @Autowired
    private WorkerSearchServiceInterface searchService;

    @PostMapping(value = { "/search" })
    public ResponseEntity<Response<List<SortItem>>> dataSearch(@RequestBody WorkerCoreQuery coreQuery) {
        QueryRequest queryRequest = QueryRequest.builder().coreQuery(coreQuery).build();
        return ResponseEntity.ok(Response.success(searchService.handle(queryRequest)));
    }
}
