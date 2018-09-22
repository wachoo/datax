package com.alibaba.datax.plugin.writer.rediswriter.transformer.impl.kv;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.rediswriter.RedisErrorCode;
import com.alibaba.datax.plugin.writer.rediswriter.ValidUtils;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.ColumnConfig;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.impl.AbsValueTransformer;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * @desc: 自定协议转换
 * @author: YanMeng
 * @date: 18-8-1
 */
public class CustomizeValueTransformer extends AbsValueTransformer{

	private String customizeSeparator;

	public CustomizeValueTransformer(String customizeSeparator) {
		this.customizeSeparator = customizeSeparator;
	}

	@Override
	public Object transform(Record record, List<ColumnConfig> columnConfig) {
		CustomizeSupport support = CustomizeSupport.getInstance(columnConfig);
		columnConfig.forEach(config -> {
			if(config.getIndex() == null){
				support.setValue(columnConfig.indexOf(config), config.getValue());
			}else{
				Column column = record.getColumn(config.getIndex());
				if(column != null){     //column为空时不设置,因CustomizeSupport所有位置都被初始化为空字符串, 所以实际上为空时默认转换为空字符串
					support.setValue(columnConfig.indexOf(config), column.asString());
				}
			}
		});
		return support.toValueStr(customizeSeparator);
	}

	static class CustomizeSupport{

		List<String> customizeValue;

		public CustomizeSupport(Integer valueCapacity) {
			ValidUtils.condition(
					valueCapacity != null && valueCapacity > 0,
					RedisErrorCode.ILLEGAL_PARAMETER_VALUE,
					"自定协议转换异常:字段容量大小设置不能为空且需要大于1");

			this.customizeValue = Lists.newArrayListWithCapacity(valueCapacity);
			for(int i = 0; i < valueCapacity; i++){
				customizeValue.add("");
			}
		}

		public static CustomizeSupport getInstance(List<ColumnConfig> columnConfig){
			Integer size = columnConfig.size();
			return new CustomizeSupport(size);
		}

		public void setValue(Integer index, String value){
			customizeValue.set(index, value);
		}

		public String toValueStr(String separator){
			return StringUtils.join(customizeValue, separator);
		}
	}
}
