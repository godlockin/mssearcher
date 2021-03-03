package com.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

@Slf4j
@Component
public class HostUtils {

    private static String ip;
    private static String hostName;

    public static String getIp() {
        if (StringUtils.isBlank(ip)) {
            synchronized (HostUtils.class) {
                if (StringUtils.isBlank(ip)) {
                    initHostInfo();
                }
            }
        }
        return ip;
    }

    public static String getHostName() {
        if (StringUtils.isBlank(hostName)) {
            synchronized (HostUtils.class) {
                if (StringUtils.isBlank(hostName)) {
                    initHostInfo();
                }
            }
        }
        return hostName;
    }

    private static void initHostInfo() {
        InetAddress localHost = null;
        try {
            localHost = Inet4Address.getLocalHost();
        } catch (UnknownHostException e) {
            log.error(e.getMessage(), e);
        }

        if (Objects.equals(null, localHost)) {
            return;
        }

        ip = localHost.getHostAddress();
        hostName = localHost.getHostName();
    }
}
