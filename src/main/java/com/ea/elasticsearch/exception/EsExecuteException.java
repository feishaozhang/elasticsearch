package com.ea.elasticsearch.exception;

/**
 *  请求Elastic Search 服务失败
 */
public class EsExecuteException extends RuntimeException{
	
	private static final long serialVersionUID = 1L;

	public EsExecuteException(String throwable) {
		super(throwable);
	}
}
