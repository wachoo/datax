package com.alibaba.datax.plugin.writer.rediswriter.transformer.impl.kv;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.ColumnConfig;
import com.alibaba.datax.plugin.writer.rediswriter.transformer.impl.AbsValueTransformer;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

/**
 * @desc: json转换
 * @author: YanMeng
 * @date: 18-8-1
 */
public class JsonValueTransformer extends AbsValueTransformer{

	@Override
	public Object transform(Record record, List<ColumnConfig> columnConfig) {
		JSONObject jsonObject = new JSONObject();

		columnConfig.forEach(config -> {
			Integer index = config.getIndex();
			String value = config.getValue();

			Column column = null;
			if(null != index && null != record.getColumn(index)){
				column = record.getColumn(index);
			}else{
				column = new StringColumn(value);
			}
			column2Type(jsonObject, column, config);
		});

		return jsonObject.toJSONString();
	}

	private void column2Type(JSONObject jsonObject, Column column, ColumnConfig config){
		switch (config.getType()){
			case BAD:
				break;
			case BOOL:
				jsonObject.put(config.getName(), column.asBoolean());    break;
			case BYTES:
				jsonObject.put(config.getName(), column.asBytes());      break;
			case DATE:
				jsonObject.put(config.getName(), config.getSdf().format(column.asDate()));       break;
			case DOUBLE:
				jsonObject.put(config.getName(), column.asDouble());     break;
			case INT:
				jsonObject.put(config.getName(), column.asLong());       break;
			case LONG:
				jsonObject.put(config.getName(), column.asLong());       break;
			case NULL:
				jsonObject.put(config.getName(), null);                  break;
			case STRING:
				jsonObject.put(config.getName(), column.asString());     break;
		}
	}

	public static void main(String[] args) {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("abc", 123L);
		System.out.println(jsonObject.toJSONString());
	}
}
