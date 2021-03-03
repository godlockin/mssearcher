package com.master.service;

import com.model.input.CoreQuery;
import com.model.input.QueryRequest;
import com.service.CacheAbleCommonInterface;

public interface QuService extends CacheAbleCommonInterface<CoreQuery, QueryRequest> { }