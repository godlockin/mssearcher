package com.model.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ESConfigBean {
    private String esAddress;
    private String esAuthUserName;
    private String esAuthPassword;

    private int bulkSize;
    private int bulkFlush;
    private int socketTimeout;
    private int bulkConcurrent;
    private int connectTimeout;
    private int connectRequestTimeout;
}