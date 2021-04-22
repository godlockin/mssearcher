package com.service;

import com.common.OperatorConfigCache;
import com.common.SysConfigUtil;
import org.apache.commons.lang3.ObjectUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * base interface for almost all services in whole system, define the base functions (with default action)
 * @param <T> the input param
 * @param <R> the output data
 */
public interface CommonInterface<T, R> {

    /**
     * register the operator into cache:
     * # save the config into SysConfigUtil
     * # save the confidence for each operator into OperatorConfigCache
     * @return operator instance itself
     */
    default CommonInterface operatorRegister() {
        SysConfigUtil.set(operatorName(), findConfig());
        OperatorConfigCache.add(handlerKey(), confidence());
        return this;
    }

    /**
     * get the operator name, default as the class's simple name
     * @return operator's name, mostly used as part of config key
     */
    default String operatorName() { return getClass().getSimpleName(); }

    /**
     * mark the operator in action, mostly used for some children operators
     * @return the operator in active flag
     */
    default Boolean operatorActive() {
        return SysConfigUtil.getAsBoolean(operatorName(), "OPERATOR_ACTIVE", false);
    }

    /**
     * the config info in system init process
     * @return core config info for each services
     */
    default Map<String, Object> findConfig() { return new LinkedHashMap<>(); }

    /**
     * the confidence for each operators, will probably used for score calculation
     * @return confidence (calc weight/rate)
     */
    default Double confidence() { return SysConfigUtil.getAsDouble(operatorName(), "CONFIDENCE", 1D); }
    /**
     * if some data callback was calling remote service, this is the url for remote service
     * @return remote url
     */
    default String remoteUrl() { return SysConfigUtil.getAsString(operatorName(), "REMOTE_URL", ""); }

    /**
     * if some data callback was calling remote service, this is the success code if remote service returns
     * @return success response code
     */
    default String ssCode() { return SysConfigUtil.getAsString(operatorName(), "SS_CODE", ""); }

    /**
     * if some data callback was calling remote service, this is the response code key in response data structure
     * @return response code key
     */
    default String ssCodeKey() { return SysConfigUtil.getAsString(operatorName(), "SS_CODE_KEY", ""); }

    /**
     * if some data callback was calling remote service, this is the response data key in response data structure
     * @return response data key
     */
    default String resultDataKey() { return SysConfigUtil.getAsString(operatorName(), "RESULT_DATA_KEY", ""); }

    /**
     * the handler name for each operator, different from operator name, this will be the official name/key for the operator
     * @return handler name
     */
    default String handlerKey() { return SysConfigUtil.getAsString(operatorName(), "HANDLER_KEY", operatorName()); }

    /**
     * the timeout for each data process
     * @return timeout in millis
     */
    default int timeout() { return SysConfigUtil.getAsInteger(operatorName(), "TIMEOUT", 2000); }

    /**
     * abstract handle function in whole system
     * @param param input for the service
     * @return output from the service
     */
    R handle(T param);

    /**
     * the default response for service, will return when process doesn't finish successfully
     * @return default response
     */
    default R defaultResult() { return (R) new Object(); }

    /**
     * the parameter's validation
     * @param param input param
     * @return whether the input param is validated or not
     */
    default boolean invalidatedParam(T param) { return ObjectUtils.isEmpty(param); }
}