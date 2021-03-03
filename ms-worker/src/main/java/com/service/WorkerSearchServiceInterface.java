package com.service;

import com.model.SortItem;
import com.model.input.QueryRequest;

import java.util.List;

public interface WorkerSearchServiceInterface extends CacheAbleCommonInterface<QueryRequest, List<SortItem>> { }