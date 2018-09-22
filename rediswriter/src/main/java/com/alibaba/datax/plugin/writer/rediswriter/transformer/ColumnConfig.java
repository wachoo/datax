package com.alibaba.datax.plugin.writer.rediswriter.transformer;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.element.Column.Type;
import com.alibaba.datax.plugin.writer.rediswriter.RedisErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.alibaba.datax.plugin.writer.rediswriter.Constant.DEFAULT_DATA_FORMAT;

/**
 * @desc:
 * @author: YanMeng
 * @date: 18-8-1
 */
public class ColumnConfig {


	/*

		"parameter":{
		    "storegeType":"kv",
		    "format":"json/customize/protobuf",
		    "keyColumnIndex":1,
		    "customizeSeparator":"\u0001",       //只用于customize
		    "column":[
		        {
		            "index": 1,
		            "value":"",
		            "type": "long",
		            "name": "foo",
		            "format": ""
		        }
		    ],
		    ""
		}
	 */
	private Type type;        //数据类型，必填
	private String name;        //字段名称，必填
	private Integer index;      //Record对应列下标
	private String value;       //特定值，和index只需要一个
	private SimpleDateFormat sdf;

	public static ColumnConfig buildByConfig(Configuration each){
		ColumnConfig config = new ColumnConfig();
		config.index = each.getInt("index");
		config.value = each.getString("value");
		config.type = each.getString("type") == null ? null : Type.valueOf(each.getString("type").toUpperCase());
		config.name = each.getString("name");

		if(Type.DATE.equals(config.type)){
			String datetimeFormat = each.getString("format");
			if(StringUtils.isNotEmpty(datetimeFormat)){
				try {
					config.sdf = new SimpleDateFormat(datetimeFormat);
					config.sdf.format(new Date());
				}catch (Exception e){
					throw DataXException.asDataXException(
							RedisErrorCode.ILLEGAL_PARAMETER_VALUE, String.format("错误的日期格式: %s", datetimeFormat), e);
				}
			}else{
				config.sdf = new SimpleDateFormat(DEFAULT_DATA_FORMAT);
			}
		}

		return config;
	}

	public SimpleDateFormat getSdf() {
		return sdf;
	}

	public void setSdf(SimpleDateFormat sdf) {
		this.sdf = sdf;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getIndex() {
		return index;
	}

	public void setIndex(Integer index) {
		this.index = index;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
