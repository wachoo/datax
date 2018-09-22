package com.alibaba.datax.plugin.writer.rediswriter.common;

/**
 * @desc: value存储格式, 仅在kv格式时生效
 * @author: YanMeng
 * @date: 18-8-1
 */
public enum  Format {
	json,       // kv存储, value格式为json
	customize,  // kv存储, value格式为分隔符自定义格式
	protobuf,   // kv存储, google protobuf格式, TODO
	hash,       // hash存储, TODO
	list        // list存储, TODO
}
