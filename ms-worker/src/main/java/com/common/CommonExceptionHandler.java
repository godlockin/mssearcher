package com.common;

import com.model.output.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@ControllerAdvice
public class CommonExceptionHandler {

    @ResponseBody
    @ExceptionHandler(value = Exception.class)
    public <T> ResponseEntity<Response<T>> exceptionHandle(HttpServletRequest request, Exception e) {
        e.printStackTrace();
        log.error("Error happened on url:[{}]", request.getRequestURI());

        ResponseEntity.BodyBuilder bodyBuilder = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        Response wrapper = Response.failure(e.getMessage());
        if (e instanceof MethodArgumentNotValidException) {
            MethodArgumentNotValidException me = (MethodArgumentNotValidException) e;
            String errMsg = buildErrMsgByBindingError(me.getBindingResult().getAllErrors());
            wrapper = Response.failure("参数校验失败：" + errMsg);
        } else if (e instanceof IllegalArgumentException) {
            IllegalArgumentException ie = (IllegalArgumentException) e;
            String errMsg = String.format("[%s]", ie.getLocalizedMessage());
            wrapper = Response.failure("参数校验失败：" + errMsg);
        } else if (e instanceof BindException) {
            BindException be = (BindException) e;
            String errMsg = buildErrMsgByBindingError(be.getAllErrors());
            wrapper = Response.failure("参数校验失败：" + errMsg);
        } else if (e instanceof HttpMessageNotReadableException) {
            wrapper = Response.failure("参数校验失败：requestBody 不存在");
        }

        return bodyBuilder.body(wrapper);
    }

    private String buildErrMsgByBindingError(List<ObjectError> objectErrors) {
        return objectErrors
                .stream()
                .map(ObjectError::getDefaultMessage)
                .map(error -> String.format("[%s]", error))
                .collect(Collectors.joining(", "));
    }
}
