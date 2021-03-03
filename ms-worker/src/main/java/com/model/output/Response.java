package com.model.output;

import com.common.constants.ResultEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Response<T> {

    private int code;

    private String message;

    private T data;

    public Response(ResultEnum resultEnum, T data) {
        this.code = resultEnum.getCode();
        this.message = resultEnum.getMessage();
        this.data = data;
    }

    public static <T> Response<T> success(T data) {
        return new Response<>(ResultEnum.SUCCESS, data);
    }

    public static <T> Response<T> success(String message, T data) {
        return new Response<>(ResultEnum.SUCCESS.getCode(), message, data);
    }

    public static <T> Response<T> failure(ResultEnum resultEnum) {
        return new Response<>(resultEnum, null);
    }

    public static <T> Response<T> failure(int code, String message) {
        return new Response<>(code, message, null);
    }

    public static <T> Response<T> failure(String message) {
        return new Response<>(ResultEnum.FAILURE.getCode(), message, null);
    }

}