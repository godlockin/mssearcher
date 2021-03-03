package com.master.service;

import com.model.SortItem;
import com.model.input.QueryRequest;
import com.service.CacheAbleCommonInterface;

import java.util.List;

public interface SearchService extends CacheAbleCommonInterface<QueryRequest, List<SortItem>> { }