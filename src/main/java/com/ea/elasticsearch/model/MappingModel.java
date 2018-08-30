package com.ea.elasticsearch.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 映射字段
 */
public class MappingModel {
	/**
	 * 变量名称
	 */
	private String fieldName;
	
	private Map<String, String> properties = new HashMap<>();

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}
	
	
}
