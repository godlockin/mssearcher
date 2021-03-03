package com.exception;

import com.common.constants.ResultEnum;
import lombok.Data;

@Data
public class MsWorkerException extends RuntimeException {

    private ResultEnum resultEnum;

    public MsWorkerException(ResultEnum resultEnum) {
        super(resultEnum.getMessage());
        this.resultEnum = resultEnum;
    }

    public MsWorkerException(ResultEnum resultEnum, String message) {
        super(message);
        this.resultEnum = resultEnum;
    }

}
