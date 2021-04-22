package com.master.controller;

import com.master.service.SearchMaster;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.model.output.QueryResult;
import com.model.output.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping(value = "v1/api")
public class SearchController {

    @Autowired
    private SearchMaster searchMaster;

    @PostMapping(value = { "/search" })
    public ResponseEntity<Response<QueryResult>> dataSearch(@RequestBody WorkerCoreQuery coreQuery) {
        QueryRequest queryRequest = QueryRequest.builder().coreQuery(coreQuery).build();
        return ResponseEntity.ok(Response.success(searchMaster.handle(queryRequest)));
    }
}
