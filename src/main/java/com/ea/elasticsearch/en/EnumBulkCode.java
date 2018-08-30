package com.ea.elasticsearch.en;

public enum EnumBulkCode {
	
	SUCCESS(0,"批量处理成功"),
	FAILD(1, "批量处理失败"),
	COPLETE(2,"批量处理完成，有可能有成功或者失败")
	;
	EnumBulkCode(int code, String msg){
		this.code = code;
		this.msg = msg;
	}
	
	private int code;
	private String msg;
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	
	
}
