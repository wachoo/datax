package com.alibaba.datax.plugin.writer.rediswriter.transformer;

import com.alibaba.datax.common.element.Record;

import java.util.List;
import java.util.Map;

/**
 * @desc: 结果格式转换
 * @author: YanMeng
 * @date: 18-7-31
 */
public interface ValueTransformer {
	/**
	 *
	 * @param record 记录
	 * @param columnConfig 列信息配置
	 * @return
	 */
	Object transform(Record record, List<ColumnConfig> columnConfig);


}
