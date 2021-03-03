package com.common.datasource;

import com.alibaba.fastjson.JSON;
import com.common.constants.Constants.RedisConfig;
import com.model.config.RedisConfigBean;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.function.BiConsumer;

@Slf4j
@Component
@NoArgsConstructor
@AllArgsConstructor
public class RedisClient {

    private JedisPool pool;

    private JedisPoolConfig config;
    private Map<String, Object> configAsMap;

    public RedisClient(RedisConfigBean configBean) {
        if (null == pool) {
            synchronized (this) {
                this.config = getConfig(configBean);
                String host = Optional.ofNullable(configBean.getRedisHost()).orElse(RedisConfig.DEFAULT_REDIS_HOST);
                Integer port = Optional.ofNullable(configBean.getRedisPort()).orElse(RedisConfig.DEFAULT_REDIS_PORT);
                Integer database = Optional.ofNullable(configBean.getRedisDatabase()).orElse(RedisConfig.DEFAULT_REDIS_DATABASE);
                log.info("Init redis for: [{}:{}] on database: [{}]", host, port, database);
                pool = new JedisPool(config
                        , host
                        , port
                        , Optional.ofNullable(configBean.getProtocolTimeout()).orElse(RedisConfig.DEFAULT_REDIS_PROTOCOL_TIMEOUT)
                        , Optional.ofNullable(configBean.getRedisPassword()).orElse(RedisConfig.DEFAULT_REDIS_PASSWORD)
                        , database
                );
                configAsMap = JSON.parseObject(JSON.toJSONString(configBean));
            }
        }
    }

    private JedisPoolConfig getConfig(RedisConfigBean configBean) {

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最大空闲数
        jedisPoolConfig.setMaxIdle(Optional.ofNullable(configBean.getMaxIdle()).orElse(RedisConfig.DEFAULT_REDIS_MAX_IDLE));
        // 连接池的最大数据库连接数
        jedisPoolConfig.setMaxTotal(Optional.ofNullable(configBean.getMaxTotal()).orElse(RedisConfig.DEFAULT_REDIS_MAX_TOTAL));
        // 最大建立连接等待时间
        jedisPoolConfig.setMaxWaitMillis(Optional.ofNullable(configBean.getMaxWaitMS()).orElse(RedisConfig.DEFAULT_REDIS_MAX_WAIT_MILLIS));
        // 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
        jedisPoolConfig.setMinEvictableIdleTimeMillis(Optional.ofNullable(configBean.getMinEvictableIdleTimeMS()).orElse(RedisConfig.DEFAULT_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS));
        // 每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
        jedisPoolConfig.setNumTestsPerEvictionRun(Optional.ofNullable(configBean.getNumTestPreEvictionRun()).orElse(RedisConfig.DEFAULT_REDIS_NUM_TESTS_PER_EVICTION_RUN));
        // 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Optional.ofNullable(configBean.getTimeBetweenEvictionRunsMS()).orElse(RedisConfig.DEFAULT_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS));
        // 是否在从池中取出连接前进行检验,如果检验失败,则从池中去除连接并尝试取出另一个
        jedisPoolConfig.setTestOnBorrow(Optional.ofNullable(configBean.getTestOnBorrow()).orElse(RedisConfig.DEFAULT_REDIS_TEST_ON_BORROW));
        // 在空闲时检查有效性, 默认false
        jedisPoolConfig.setTestWhileIdle(Optional.ofNullable(configBean.getTestWhileIdle()).orElse(RedisConfig.DEFAULT_REDIS_TEST_WHILE_IDLE));

        return jedisPoolConfig;
    }

    public Map<String, Object> findConfig() { return configAsMap; }

    /**
     * set the value for saveKey in db
     *
     * @param key
     * @param value
     * @return
     */
    public String set(String key, String value) {
        String result = "";
        try (Jedis jedis = pool.getResource()) {
            result = jedis.set(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * set value for saveKey in db, set the expire time in Second
     * #force = true to set the value whether it exists
     * #force = false to set the value only if it not exists
     *
     * @param key
     * @param value
     * @param timeInSecond
     * @return
     */
    public String set(String key, String value, Long timeInSecond) {
        String result = "";

        Long expire = Optional.ofNullable(timeInSecond).orElse(0L);
        if (0 == expire) {
            return set(key, value);
        } else {
            try (Jedis jedis = pool.getResource()) {
                SetParams params = new SetParams().ex(expire.intValue());
                if (jedis.exists(key)) {
                    params.xx();
                } else {
                    params.nx();
                }

                result = jedis.set(key, value, params);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public String get(String key) {
        String result = "";

        try (Jedis jedis = pool.getResource()) {
            result = jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * append value(s) for saveKey of set
     *
     * @param key
     * @param values
     * @return
     */
    public Long sadd(String key, String... values) {
        Long result = 0L;
        try (Jedis jedis = pool.getResource()) {
            result = jedis.sadd(key, values);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * remove value(s) for saveKey of set
     *
     * @param key
     * @param values
     * @return
     */
    public Long srem(String key, String... values) {
        Long result = 0L;
        try (Jedis jedis = pool.getResource()) {
            result = jedis.srem(key, values);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * returns the items which exists in db of saveKey
     *
     * @param key
     * @param items
     * @return
     */
    public Set<String> sismember(String key, Collection<String> items) {
        Set<String> set = new HashSet<>();
        try (Jedis jedis = pool.getResource()) {
            items.stream().filter(x -> jedis.sismember(key, x)).forEach(set::add);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return set;
    }

    /**
     * In db, get the items sort by score desc, and get the item of index "start" to "end"
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<String> zrevrange(String key, long start, long end) {
        Set<String> set = new HashSet<>();
        try (Jedis jedis = pool.getResource()) {
            set = jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return set;
    }

    /**
     * return the limit numbers of elements
     *
     * @param key
     * @param limit
     * @return
     */
    public List<String> srandmember(String key, Integer limit) {
        List<String> result = new ArrayList<>();
        try (Jedis jedis = pool.getResource()) {
            result = jedis.srandmember(key, limit);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public Long zrem(String key, String... values) {
        Long result = 0L;
        try (Jedis jedis = pool.getResource()) {
            result = jedis.zrem(key, values);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * add a value with score into an ordered set
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public Long zadd(String key, Double score, String member) {
        Long result = 0L;
        score = Optional.ofNullable(score).orElse(0D);
        try (Jedis jedis = pool.getResource()) {
            result = jedis.zadd(key, score, member);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Long del(String key) {
        Long result = 0L;
        try (Jedis jedis = pool.getResource()) {
            result = jedis.del(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Double zincrby(String key, Double score, String value) {
        Double result = 0D;
        try (Jedis jedis = pool.getResource()) {
            result = jedis.zincrby(key, score, value);
            if (result.equals(Double.MAX_VALUE)) {
                jedis.zincrby(key, -1, value);
            } else if (result < 0) {
                jedis.zincrby(key, 0 - result, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * return all the items belongs to saveKey
     *
     * @param key
     * @return
     */
    public Set<String> smember(String key) {
        Set<String> set = new HashSet<>();
        try (Jedis jedis = pool.getResource()) {
            set = jedis.smembers(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return set;
    }

    /**
     * Set saveKey, value with expire time (in ms)
     *
     * @param key
     * @param expireMs
     * @param value
     * @return
     */
    public String psetex(String key, Long expireMs, String value) {
        String result = "";
        try (Jedis jedis = pool.getResource()) {
            result = jedis.psetex(key, expireMs, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Long incrBy(String key, Long num) {
        Long result = 0L;

        num = Optional.ofNullable(num).orElse(1L);
        try (Jedis jedis = pool.getResource()) {
            result = jedis.incrBy(key, num);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Boolean exists(String key) {
        Boolean exists = false;

        try (Jedis jedis = pool.getResource()) {
            exists = jedis.exists(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exists;
    }

    public Long hdel(String key, String field) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.hdel(key, field);
        } catch (Exception e) {
            // TODO: handle exception
            log.error(e.getMessage());
            return null;
        }
    }

    public String hget(String key, String field) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.hget(key, field);
        } catch (Exception e) {
            // TODO: handle exception
            log.error(e.getMessage());
            return null;
        }
    }

    public Map<String, String> hgetAll(String key) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.hgetAll(key);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new HashMap<>();
        }
    }

    public Map<String, Map> bulkHgetAll(List<String> keys) {
        BiConsumer<Pipeline, String> pipelineConsumer = PipelineBase::hgetAll;
        return bulkDoRedisCall(keys
                , pipelineConsumer
                , Map.class
                , new HashMap<>()
                , "bulkHgetAll");
    }

    private <T> Map<String, T> bulkDoRedisCall(List<String> keys
            , BiConsumer<Pipeline, String> pipelineConsumer
            , Class<T> clazz
            , T defaultValue
            , String funcName
    ) {
        Map<String, T> result = new HashMap<>();
        try (Jedis jedis = pool.getResource(); Pipeline pipeline = jedis.pipelined()) {
            Set<String> set = new HashSet<>();
            keys.stream().filter(StringUtils::isNotBlank).peek(set::add).forEach(key -> pipelineConsumer.accept(pipeline, key));
            List<Object> pipResults = pipeline.syncAndReturnAll();
            int size = set.size();
            List<String> workKeys = new ArrayList<>(set);
            for (int i = 0; i < size; i ++) {
                String workKey = workKeys.get(i);
                Object resp = pipResults.get(i);
                T respItem = defaultValue;
                if (resp instanceof JedisDataException) {
                    log.error("Error got during we do {} for key:[{}], with error:[{}]", funcName, workKey, resp);
                } else {
                    respItem = clazz.cast(resp);
                }
                result.put(workKey, respItem);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return result;
    }

    public void hset(String key, String field, String value) {
        try (Jedis jedis = pool.getResource();) {
            jedis.hset(key, field, value);
        } catch (Exception e) {
            // TODO: handle exception
            log.error(e.getMessage());
        }
    }

    public String hmset(String key, Map<String, String> info) {
        String result = "";
        try (Jedis jedis = pool.getResource()) {
            result = jedis.hmset(key, info);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }

    public String hmset(String key, Map<String, String> info, Long expireInSS) {

        String result = hmset(key, info);
        if (StringUtils.isNotBlank(result)) {
            try (Jedis jedis = pool.getResource()) {
                jedis.expire(key, expireInSS.intValue());
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
        return result;
    }

    /**
     * expire 设置超时,秒
     *
     * @param key
     */
    public void expire(String key, int expireTime) {
        try (Jedis jedis = pool.getResource();) {
            jedis.expire(key, expireTime);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @PreDestroy
    public void destroy() {
        if (ObjectUtils.isNotEmpty(pool)) {
            try {
                pool.destroy();
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened during we destroy redis pool");
            }
        }
    }
}
