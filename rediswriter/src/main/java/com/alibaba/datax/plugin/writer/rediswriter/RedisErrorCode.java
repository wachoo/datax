package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @desc:
 * @author: YanMeng
 * @date: 18-7-31
 */
public enum  RedisErrorCode implements ErrorCode {
	ILLEGAL_PARAMETER_VALUE("ILLEGAL_PARAMETER_VALUE","参数不合法"),
	ILLEGAL_CONNECT_STATE("ILLEGAL_CONNECT_STATE","连接状态异常"),
	;

	private final String code;
	private final String desc;

	RedisErrorCode(String code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	@Override
	public String getCode() {
		return null;
	}

	@Override
	public String getDescription() {
		return null;
	}
}
