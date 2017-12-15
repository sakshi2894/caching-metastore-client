package com.qubole.utility;

import com.google.common.cache.CacheLoader;
import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.SerializationUtils;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class RedisCacheTest {


  //Value gets loaded from the Cache
  @Test
  public void testGetFromCache() throws Exception {

    String keyPrefixStr = "test.";
    String key = "key";
    Integer sourceClientValue = 12;
    Integer cacheValue = 50;

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.get((keyPrefixStr + key).getBytes())).thenReturn(SerializationUtils.serialize((Serializable) cacheValue));

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(key)).thenReturn(sourceClientValue);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Integer valueObserved = (Integer) redisCache.get(key);
    assert (cacheValue.equals(valueObserved));

  }

  //jedis.get() throws an error, value gets loaded from value Loader
  @Test
  public void testGetFromSource() throws Exception {

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedis.get(anyString().getBytes())).thenThrow(new JedisConnectionException("Get call failed."));
    when(jedisPool.getResource()).thenReturn(jedis);

    String keyPrefixStr = "test.";
    String key = "key";
    Integer valueExpected = 12;

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(key)).thenReturn(valueExpected);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Integer valueObserved = (Integer) redisCache.get(key);
    assert (valueExpected.equals(valueObserved));

  }

  //jedis.get() returns null, value gets loaded from value Loader
  @Test
  public void testGetFromSourceCacheMiss() throws Exception {

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedis.get(anyString().getBytes())).thenReturn(null);
    when(jedisPool.getResource()).thenReturn(jedis);

    String keyPrefixStr = "test.";
    String key = "key";
    Integer valueExpected = 12;

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(key)).thenReturn(valueExpected);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Integer valueObserved = (Integer) redisCache.get(key);
    assert (valueExpected.equals(valueObserved));

  }




  //Exception thrown if missingcache is enabled, and the key exists in "missing" cache.
  @Test(expected = ExecutionException.class)
  public void testGetFromMissingCache() throws Exception {
    String keyPrefixStr = "test.";
    String key = "key";

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedisPool.getResource()).thenReturn(jedis);

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = true;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    when(jedis.exists(Bytes.concat(redisCache.getNotFoundPrefix(), key.getBytes()))).thenReturn(true);

    redisCache.get(key);
  }

}