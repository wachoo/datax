package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.common.Format;
import com.alibaba.datax.common.element.Column.Type;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.ColumnConfig;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.ValueTransformer;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.impl.kv.CustomizeValueTransformer;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.impl.kv.JsonValueTransformer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @desc:
 * @author: YanMeng
 * @date: 18-7-31
 */
public class RedisWriter extends Writer {

	/*
	 {
	    "name": "rediswriter",
	    "parameter":{
	        "host":"127.0.0.1",
	        "port":6397,
	        "timeout":3000,                 //redis连接超时毫秒
	        "batchSize":5000,               //redis批量写入缓存条数
	        "expire":2592000,               //redis key超时秒数
	        "format":"json/customize/protobuf/hash/list",       //redis存储格式
	        "keyColumnIndex":1,             //redis key所对应列下标
	        "customizeSeparator":"",        //只用于customize, 默认为\u0001
	        "column":[
	            {
	                "index": 1,             //列下标
	                "value":"",             //具体数值, 当index不存在时才会生效
	                "type": "long",
	                "name": "foo"
	            }
	        ]
	    }
	 }
	 */
	public static class Job extends Writer.Job {

		private Configuration originalConfig = null;

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			List<Configuration> configList = Lists.newArrayList();
			for(int i = 0; i < mandatoryNumber; i++) {
				configList.add(this.originalConfig.clone());
			}
			return configList;
		}

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
		}

		@Override
		public void prepare() {
			super.prepare();
		}

		@Override
		public void destroy() {

		}
	}

	public static class Task extends Writer.Task {

		private static final Logger logger = LoggerFactory.getLogger(Task.class);
		private Configuration writerConfig;

		//redis 配置信息
		private String host;                //redis host
		private Integer port;               //redis port
		private Integer timeout;            //redis 连接/请求超时时间
		private Integer batchSize;          //redis 写入批次大小, 每次达到batchSize后缓存数据真正被刷到redis
		private Integer expire;             //redis 写入数据超时时间, 单位:s
		//数据格式 配置信息
		private Format format;              //value数据格式, 支持json/自定义格式/protobuf
		private Integer keyColumnIndex;     //key在record中的下标
		private String customizeSeparator;  //自定义分隔符, 仅在format为自定义格式时支持该配置, 默认为ascii中的 \u0001
		//列信息 配置信息
		/** @see com.alibaba.datax.plugin.writer.rediswriter.transformer.ColumnConfig */
		private List<ColumnConfig> columnConfigList;            //列配置信息

		//运行时对象
		private Jedis jedis = null;
		private Pipeline pipeline = null;                       //redis pipeline, 用于批量写入数据
		private ValueTransformer valueTransformer = null;       //格式转换

		@Override
		public void init() {
			//获取、解析配置
			this.writerConfig = this.getPluginJobConf();

			host = writerConfig.getString("host");
			port = writerConfig.getInt("port");
			timeout = writerConfig.getInt("timeout");
			batchSize = writerConfig.getInt("batchSize");
			expire = writerConfig.getInt("expire");
			format = Format.valueOf(writerConfig.getString("format"));
			keyColumnIndex = writerConfig.getInt("keyColumnIndex");
			customizeSeparator = writerConfig.getString("customizeSeparator");
			columnConfigList = writerConfig.getListConfiguration("column")
					.stream()
					.map(ColumnConfig::buildByConfig)
					.collect(Collectors.toList());

			//验证或赋予默认值
			validVerify();

			//创建、初始化运行时对象
			jedis = RedisUtils.getJedis(host, port, timeout);
			pipeline = jedis.pipelined();

			//创建格式转换器
			Map<String, Object> params = new HashedMap();
			params.put("customizeSeparator", customizeSeparator);
			valueTransformer = TransformerFactory.getInstance(format, params);
		}

		@Override
		public void destroy() {
			if(pipeline != null){
				IOUtils.closeQuietly(pipeline);
			}
			if(jedis != null){
				IOUtils.closeQuietly(jedis);
			}
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			Map<String, Object> writerBuffer = Maps.newHashMap();
			Record record = null;
			while((record = lineReceiver.getFromReader()) != null) {
				writerBuffer.put(record.getColumn(keyColumnIndex).asString(),valueTransformer.transform(record, columnConfigList));
				if(writerBuffer.size() >= this.batchSize) {
					doBatchPut(writerBuffer, true);
					writerBuffer.clear();
				}
			}
			if(!writerBuffer.isEmpty()) {
				doBatchPut(writerBuffer, true);
				writerBuffer.clear();
			}
		}

		private void doBatchPut(Map<String, Object> writerBuffer, Boolean isFirst){
			try {
				for(String k : writerBuffer.keySet()){
					if(StringUtils.isNotBlank(k)){
						Object v = writerBuffer.get(k);
						String vStr = v == null ? "" : v.toString();
						if (expire == null){
							pipeline.set(k, vStr);
						}else{
							pipeline.setex(k, expire, vStr);
						}
					}
				}

				pipeline.sync();

			} catch (Exception e){
				if(isFirst){
					logger.warn("redis连接异常,尝试重连!");
					try {
						destroy();
						jedis = RedisUtils.getJedis(host, port, timeout, 5);
						pipeline = jedis.pipelined();
					}catch (Exception e1){
						logger.warn("redis连接异常,重连失败!");
						throw e1;
					}
					logger.warn("redis连接异常,重连已完成!");

					//重新执行导入
					doBatchPut(writerBuffer, false);

				}else{
					throw DataXException.asDataXException(
							RedisErrorCode.ILLEGAL_CONNECT_STATE, e);
				}
			}
		}

		private void validVerify(){

			ValidUtils.condition(
					StringUtils.isNotEmpty(host),
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					"host 不能为空!");
			ValidUtils.condition(
					port != null && port > 1024 && port < 65535,
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					String.format("port 格式不合法, port - %d", port));
			ValidUtils.condition(
					keyColumnIndex != null && keyColumnIndex >= 0,
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					String.format("keyColumnIndex 格式不合法, keyColumnIndex - %d, 需不能为空且不为负数(下标从0开始)", keyColumnIndex));
			ValidUtils.condition(
					columnConfigList != null && !columnConfigList.isEmpty(),
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					"column 不能为空!");

			ValidUtils.isNotNull(
					format,
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					String.format("format 格式不合法, 允许的枚举:%s", Format.values()));

			//TODO 扩展Format支持后去掉该验证
			ValidUtils.condition(
					format == Format.json || format == Format.customize,
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					"format 目前仅支持json和customize格式");

			//部分字段设置默认值
			if(timeout == null || timeout <= 0){
				timeout = 2000;
			}
			if(batchSize == null || batchSize <= 0){
				batchSize = 6000;
			}
			if(format == Format.customize && StringUtils.isEmpty(customizeSeparator)){
				customizeSeparator = "\u0001";
			}

			//检查column配置
			columnConfigList.forEach(columnConfig -> {
				ValidUtils.isNotNull(
						columnConfig.getType(),
						RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
						String.format("column.type 格式不合法, 不能为空且允许的枚举为:%s", Type.values()));

				ValidUtils.condition(
						StringUtils.isNotEmpty(columnConfig.getName()),
						RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
						"column.name 不能为空!");

				ValidUtils.condition(
						columnConfig.getIndex() == null && StringUtils.isNotEmpty(columnConfig.getValue()) ||
									columnConfig.getIndex() != null && StringUtils.isEmpty(columnConfig.getValue()),
						RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
						String.format("column.index和column.value不能均为空且不能共存, index - %d, value - %s", columnConfig.getIndex(), columnConfig.getValue()));
			});
		}
	}

	/**
	 * redis value 格式转换器 工厂类
	 */
	static class TransformerFactory{
		Format format;                  //存储格式
		Class<? extends ValueTransformer> clazz;    //转换器类全路径

		/**
		 * 支持被重写, 用于创建构造方法不同的实例
		 * @param params
		 * @return
		 * @throws NoSuchMethodException
		 * @throws IllegalAccessException
		 * @throws InvocationTargetException
		 * @throws InstantiationException
		 */
		protected ValueTransformer getInstance(Map<String, Object> params) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			return this.clazz.getConstructor().newInstance();
		}

		private TransformerFactory(Format format, Class<? extends ValueTransformer> clazz) {
			this.format = format;
			this.clazz = clazz;
		}

		/**
		 * 静态方法, 用于获取value转换器实例
		 * @param format
		 * @param params
		 * @return
		 */
		public static ValueTransformer getInstance(Format format, Map<String, Object> params){
			TransformerFactory targetFactory = null;
			for(TransformerFactory factory : factorys){
				if(factory.format.equals(format)){
					targetFactory = factory;
				}
			}

			ValidUtils.condition(
					targetFactory != null,
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					String.format("获取格式转换器失败: 不匹配的数据格式%s", format.name()));

			try {
				return targetFactory.getInstance(params);
			} catch (Exception e) {
				throw DataXException.asDataXException(
						RedisErrorCode.ILLEGAL_PARAMETER_VALUE, e);
			}
		}

		/**
		 * 工厂类集合
		 */
		static List<TransformerFactory> factorys = Lists.newArrayList();

		/**
		 * 工厂类集合初始化
		 */
		static{
			factorys.add(new TransformerFactory(Format.json, JsonValueTransformer.class));
			factorys.add(new TransformerFactory(Format.customize, CustomizeValueTransformer.class){
				@Override
				protected ValueTransformer getInstance(Map<String, Object> params) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
					ValidUtils.condition(
							params.get("customizeSeparator") != null && params.get("customizeSeparator").toString().trim() != "",
							RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
							String.format("获取格式转换器失败: 自定义格式的转换器 构造的customizeSeparator参数为必需参数"));

					return this.clazz.getConstructor(String.class).newInstance(params.get("customizeSeparator"));
				}
			});
		}
	}

}
