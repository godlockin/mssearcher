package com.config;

import com.common.constants.ResultEnum;
import com.exception.MsWorkerException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
public class GlobalTasksPool {
    private GlobalTasksPool() {
        throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
    }

    private static ThreadPoolTaskExecutor executor;
    static {
        executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(50);
        executor.setMaxPoolSize(1024);
        executor.setQueueCapacity(50);
        executor.setKeepAliveSeconds(60);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
    }

    public static ThreadPoolTaskExecutor getExecutor() { return executor; }

}
