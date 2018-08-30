package com.ea.elasticsearch.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.ea.elasticsearch.model.MappingModel;

/**
 * Model 生成工具
 * 
 */
public class GenerateTool {

	/**
	 * 根据mappingModelList生成索引映射
	 * @param mappingModelList
	 * @throws Exception
	 */
	public static XContentBuilder generateMappingModel(List<MappingModel> mappingModelList) throws Exception {
		XContentBuilder mapping = XContentFactory.jsonBuilder();
		
		mapping = mapping.startObject().startObject("properties");
		if (mappingModelList != null && mappingModelList.size() > 0) {
			for (MappingModel mappingModel : mappingModelList) {
				if(mappingModel == null) {
					continue;
				}
				String fieldName = mappingModel.getFieldName();
				if (StringUtils.isNotBlank(fieldName)) {
					mapping = mapping.startObject(fieldName);
					Iterator<Map.Entry<String, String>> properties = mappingModel.getProperties().entrySet().iterator();
					if(properties == null) {
						continue;
					}
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
	
	
	/**
	 * 设置index的Settings
	 */
	public static Settings createIndexSetting() {
		String number_of_shards = ResourceUtils.getProperty("number_of_shards");
		String number_of_replicas = ResourceUtils.getProperty("number_of_replicas");
		
		Integer numbersOfShard = null;
		Integer numberOfReplicas = null;
		
		if(StringUtils.isNotBlank(number_of_shards)) {
			try {
				numbersOfShard = Integer.parseInt(number_of_shards);
			} catch (Exception e) {
				//忽略，如果没有就按默认设置
			}
		}
		
		if(StringUtils.isNotBlank(number_of_replicas)) {
			try {
				numberOfReplicas = Integer.parseInt(number_of_replicas);
			} catch (Exception e) {
				//忽略，如果没有就按默认设置
			}
		}
		
		if(numbersOfShard == null || numberOfReplicas == null) {
			return null;
		}
		
	    Settings settings = Settings.builder()
	    		.put("number_of_shards", numbersOfShard)
	    		.put("number_of_replicas", numberOfReplicas).build();
	    return settings;
	}
}
