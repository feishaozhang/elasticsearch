package com.ea.elasticsearch.dao;

import com.ea.elasticsearch.ElasticSearchBase;

public class ElasticSearchOperator<T> extends ElasticSearchBase<T>{

	public ElasticSearchOperator(String index, String type) {
		super(index, type);
	}
	
}
