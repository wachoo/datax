package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.exception.DataXException;

/**
 * @desc:
 * @author: YanMeng
 * @date: 18-7-31
 */
public class ValidUtils {

	public static void isNotNull(Object o, RedisErrorCode errorCode, String msg){
		if(o == null){
			throw DataXException.asDataXException(errorCode, msg);
		}
	}

	public static void isNotNull(Object o, RedisErrorCode errorCode){
		isNotNull(o, errorCode, errorCode.getDescription());
	}


	public static void condition(boolean expression, RedisErrorCode errorCode, String msg){
		if(!expression){
			throw DataXException.asDataXException(errorCode, msg);
		}
	}

	public static void condition(boolean expression, RedisErrorCode errorCode){
		condition(expression, errorCode, errorCode.getDescription());
	}
}
