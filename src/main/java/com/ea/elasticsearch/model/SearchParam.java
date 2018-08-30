package com.ea.elasticsearch.model;

import java.util.List;

import org.elasticsearch.action.search.SearchType;

public class SearchParam {
	/**
	 * 索引名称
	 */
	private List<String> indexs;
	/**
	 * 类型名称
	 */
	private List<String> types;
	/**
	 * searchType
	 */
	private SearchType searchType;
	/**
	 * 查找内容
	 */
	private String query;
	/**
	 * 起始位置
	 */
	private int from;
	/**
	 * 数据长度
	 */
	private int size;
	/**
	 * 设置是否按查询匹配度排序
	 */
	private boolean setExplain;
	
}
