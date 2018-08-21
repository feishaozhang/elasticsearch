package com.ea.elasticsearch.en;

public enum EnumEsDataType {
	/**
	 * 字段类型 5.*后就不使用了用text/keyword替代
	 */
	STRING("string", "字符串"),
	/**
	 * 整形
	 */
	INTEGER("integer", "整型"),
	/**
	 * 长整型
	 */
	LONG("long", "长整型"),
	/**
	 * 单精度浮点数
	 */
	FLOAT("float", "单精度浮点数"),
	/**
	 * 双精度浮点数
	 */
	DOUBLE("double", "双精度浮点数"),
	/**
	 * bool类型
	 */
	BOOL("bool", "bool类型"),
	/**
	 * 时间
	 */
	DATE("DATE", "时间"),
	/**
	 * 二进制数
	 */
	BINARY("binary", "二进制数"),
	/**
	 * 内容文本
	 */
	TEXT("text", "内容文本"),
	/**
	 * 精确查找使用如URL
	 */
	KEYWORD("keyword", "精确查找使用如URL"),
	;
	
	EnumEsDataType(String name, String desc){
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
