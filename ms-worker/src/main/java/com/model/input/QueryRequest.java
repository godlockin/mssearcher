package com.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryRequest {

    protected QueryConfig queryConfig = new QueryConfig();
    protected WorkerCoreQuery coreQuery = new WorkerCoreQuery();
}
