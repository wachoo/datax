package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @desc:
 *  1. 获取jedis连接方法
 *  2. 带重连机制的获取连接方法
 *
 * @author: YanMeng
 * @date: 18-7-31
 */
public class RedisUtils {

	/**
	 * 获取连接
	 * @param host host
	 * @param port port
	 * @param timeout 超时事件
	 * @return
	 */
	public static Jedis getJedis(String host, Integer port, Integer timeout){

		ValidUtils.condition(StringUtils.isNotEmpty(host), RedisErrorCode.ILLEGAL_PARAMETER_VALUE);
		ValidUtils.condition(port != null && port > 1024 && port < 65535, RedisErrorCode.ILLEGAL_PARAMETER_VALUE);

		Jedis jedis = null;
		if(timeout == null){
			jedis = new Jedis(host, port);
		}else{
			jedis = new Jedis(host, port, timeout);
		}

		browse(jedis);

		return jedis;
	}

	/**
	 * 获取连接, 失败时重试固定次数
	 * @param host host
	 * @param port port
	 * @param timeout 超时事件
	 * @param retry 重试次数
	 * @return
	 */
	public static Jedis getJedis(String host, Integer port, Integer timeout, Integer retry){

		if(retry == null || retry <= 0){
			retry = 1;
		}

		for (int i = 0; i < retry; i++){
			try {
				return getJedis(host, port, timeout);
			} catch (DataXException e){
				if(i < retry  - 1){
					threadSleep(5000L);
				}else if(i == retry  - 1){
					throw DataXException.asDataXException(RedisErrorCode.ILLEGAL_CONNECT_STATE,
							String.format("重试连接redis异常, 重试次数:%d, 异常原因: %s", retry, e.getMessage()));
				}
			}
		}
		return null;
	}


	/**
	 * 测试连接是否正常
	 * @param jedis
	 */
	private static void browse(Jedis jedis){
		String pong = null;
		try {
			pong = jedis.ping();
		}catch (JedisConnectionException e){
			throw DataXException.asDataXException(RedisErrorCode.ILLEGAL_CONNECT_STATE,
					String.format("连接redis异常, 请检查连接配置, 异常原因: %s", e.getMessage()), e);
		}
		ValidUtils.condition("PONG".equals(pong), RedisErrorCode.ILLEGAL_CONNECT_STATE,
					String.format("连接redis异常, ping结果: %s", pong));
	}

	private static void threadSleep(long millis){
		try {
			Thread.currentThread().sleep(millis);
		} catch (InterruptedException e) {
		}
	}
}
