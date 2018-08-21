package com.ea.elasticsearch.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.ea.elasticsearch.model.MappingModel;

public class GenerateTool {

	/**
	 * 生成映射MODEL
	 * @param mappingModelList
	 * @throws Exception
	 */
	public static XContentBuilder generateMappingModel(List<MappingModel> mappingModelList) throws Exception {
		XContentBuilder mapping = XContentFactory.jsonBuilder();
		
		mapping = mapping.startObject().startObject("properties");
		if (mappingModelList != null && mappingModelList.size() > 0) {
			for (MappingModel mappingModel : mappingModelList) {
				String fieldName = mappingModel.getFieldName();
				if (StringUtils.isNotBlank(fieldName)) {
					mapping = mapping.startObject(fieldName);
					Iterator<Map.Entry<String, String>> properties = mappingModel.getProperties().entrySet().iterator();
					while (properties.hasNext()) {
						Entry<String, String> next = properties.next();
						if (next != null) {
							String key = next.getKey();
							String value = next.getValue();
							//这里可以使用枚举过滤不存在的属性key
							if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
								mapping = mapping.field(key, value);
							}
						}
					}
					mapping.endObject();
				}
			}

		}
		mapping.endObject().endObject();
		
		return mapping;
	}
}
