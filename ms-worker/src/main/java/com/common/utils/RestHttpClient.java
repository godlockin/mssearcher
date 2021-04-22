package com.common.utils;

import com.alibaba.fastjson.JSON;
import com.common.constants.ResultEnum;
import com.exception.MsWorkerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
@Component
public class RestHttpClient {

    private static final RequestConfig requestConfig = RequestConfig.DEFAULT;

    private static CloseableHttpClient closeableHttpClient;

    private RestHttpClient() {
        throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
    }

    public RestHttpClient(@Qualifier("closeableHttpClient") CloseableHttpClient client) {
        closeableHttpClient = client;
    }

    public static String doPost(String url, Map<String, Object> param) throws MsWorkerException {
        log.info("Do post to url:[{}] by param:{}", url, JSON.toJSONString(param));
        PostHttp post = new PostHttp(url);
        StringEntity entity = new StringEntity(JSON.toJSONString(param), StandardCharsets.UTF_8);
        entity.setContentType(MediaType.APPLICATION_JSON_UTF8_VALUE);
        post.setHeader("Content-type", "application/json");
        post.setEntity(entity);
        return doCall(post);
    }

    public static String doGet(String url) throws MsWorkerException {
        log.info("Do get to url:[{}]", url);
        return doCall(new GetHttp(url));
    }

    private static String doCall(HttpRequestBase httpRequestBase) throws MsWorkerException {
        // 装载配置信息
        try (CloseableHttpResponse response = closeableHttpClient.execute(httpRequestBase)) {
            String resp = "";
            StatusLine status = response.getStatusLine();
            if (200 == status.getStatusCode()) {
                resp = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            } else {
                log.warn("Got response:{}", status);
            }
            return resp;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened on calling:[{}], {}", httpRequestBase.getURI(), e);
            throw new MsWorkerException(ResultEnum.FAILURE);
        }
    }

    private static class PostHttp extends HttpPost {
        PostHttp(String url) {
            super(url);
            super.setConfig(requestConfig);
        }
    }

    private static class GetHttp extends HttpGet {
        GetHttp(String url) {
            super(url);
            super.setConfig(requestConfig);
        }
    }
}
