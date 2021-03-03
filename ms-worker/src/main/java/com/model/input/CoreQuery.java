package com.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoreQuery {

    protected int pageNo = 1;
    protected int pageSize = 100;
    protected String query = "";
}
