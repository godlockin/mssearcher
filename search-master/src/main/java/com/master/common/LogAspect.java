package com.master.common;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Aspect
@Component
public class LogAspect {

    private ConcurrentHashMap<Long, Long> cache = new ConcurrentHashMap<>();
    private List<String> IGNORE_URL = Arrays.asList("/", "/health");

    @Pointcut("execution(public * com.master.controller.*.*(..))")
    public void logic() {}

    @Before("logic()")
    public void doBeforeLogic(JoinPoint joinPoint) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        if (IGNORE_URL.contains(request.getRequestURI())) {
            return;
        }

        long threadId = Thread.currentThread().getId();
        long currTime = System.currentTimeMillis();
        log.info("Thread:[{}] of ip:[{}] is trying to visit url:[{}] as method:[{}], which mapping to:[{}] with args:[{}] at:[{}]",
                threadId, request.getRemoteAddr(), request.getRequestURI(), request.getMethod(),
                joinPoint.getSignature().getDeclaringTypeName() + "," + joinPoint.getSignature().getName(),
                (null == joinPoint.getArgs()) ? "" : JSON.toJSON(joinPoint.getArgs()), currTime);
        cache.put(threadId, currTime);
    }

    @AfterReturning(returning = "object", pointcut = "logic()")
    public void doAfterLogic(Object object) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        if (IGNORE_URL.contains(request.getRequestURI())) {
            return;
        }

        long threadId = Thread.currentThread().getId();
        long currTime = System.currentTimeMillis();
        long fromTime = Optional.ofNullable(cache.get(threadId)).orElse(0L);
        log.info("Thread:[{}] Finished visit url:[{}] as method:[{}] at:[{}] took:[{}]",
                threadId, request.getRequestURI(), request.getMethod(), currTime, (currTime - fromTime));
        cache.remove(threadId);
    }

    @Pointcut("execution(* com.*.*(..))")
    public void everyLogic() {}

    @Around("everyLogic()")
    public Object doAroundEveryLogic(ProceedingJoinPoint pjp) throws Throwable {
        long start = System.nanoTime();
        Object result = pjp.proceed();
        long end = System.nanoTime();

        Signature signature = pjp.getSignature();
        log.info("Called [{}]-[{}] took [{}]"
                , Optional.ofNullable(signature.getDeclaringTypeName()).orElse("")
                , Optional.ofNullable(signature.getName()).orElse("")
                , (end - start) / 1_000_000
                );

        return result;
    }

    @Pointcut("execution(public * com.master.service.impl.*Service*.*(..))")
    public void queryHandler() {}

    @Around("queryHandler()")
    public Object doAroundQueryHandler(ProceedingJoinPoint pjp) throws Throwable {
        long start = System.nanoTime();
        Object result = pjp.proceed();
        long end = System.nanoTime();

        Signature signature = pjp.getSignature();
        log.info("Called [{}]-[{}] took [{}]"
                , Optional.ofNullable(signature.getDeclaringTypeName()).orElse("")
                , Optional.ofNullable(signature.getName()).orElse("")
                , (end - start) / 1_000_000
        );

        return result;
    }
}
