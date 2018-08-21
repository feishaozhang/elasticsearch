package com.ea.elasticsearch.en;

public enum EnumMappingProperties {
	/**
	 * 字段类型
	 */
	TYPE("type", "字段类型"),
	/**
	 * 存储分词器
	 */
	ANALYZER("analyzer", "分词器"),
	/**
	 * 搜索分词器
	 */
	SEARCH_ANALYZER("asearch_analyzernalyzer", "搜索分词器"),
	/**
	 * 在进行搜索时，如果不指明要搜索的文档的域，ElasticSearch则会去搜索_all域。_all带来搜索方便，其代价是增加了系统在索引阶段对CPU和存储空间资源的开销。
	 */
	INCLUDE_IN_ALL("include_in_all", "是否加入到_all中"),
	;
	
	
	EnumMappingProperties(String name, String desc){
		this.name = name;
		this.desc = desc;
	}
	
	String name;
	String desc;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	
}
