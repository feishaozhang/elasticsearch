package com.ea.elasticsearch.dao;

import com.ea.elasticsearch.ElasticSearchBase;

/**
 * 获取聊天记录DAO
 */
public class RecordDao extends ElasticSearchBase{
	
	//构造函数传入索引，以及类型
	public RecordDao() {
		super("customertest", "doc");
	}
	
	
}
