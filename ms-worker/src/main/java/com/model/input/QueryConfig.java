package com.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryConfig {

    protected double globalRate = 1D;
    protected Map<String, Double> rateMapping = new HashMap<>();
}