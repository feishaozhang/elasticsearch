package com.ea.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.ea.elasticsearch.dao.RecordDao;
import com.ea.elasticsearch.en.EnumEsDataType;
import com.ea.elasticsearch.en.EnumMappingProperties;
import com.ea.elasticsearch.exception.EsExecuteException;
import com.ea.elasticsearch.model.MappingModel;
import com.ea.elasticsearch.model.Record;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
	/**
	 * 生成索引
	 * @throws EsExecuteException 
	 */
	public void  testGenerateIndex() throws EsExecuteException{
		ElasticSearchBase base = new ElasticSearchBase("shaofei","shaofei");
		//映射属性集
		ArrayList<MappingModel> arrayList = new ArrayList<MappingModel>();
		
		for (int i = 0; i < 10; i++) {
			MappingModel m = new MappingModel();
			m.setFieldName("shaofei"+i);
			HashMap<String, String> hashMap = new HashMap<String, String>();
			hashMap.put(EnumMappingProperties.TYPE.getName(), EnumEsDataType.TEXT.getName());
			m.setProperties(hashMap);
			arrayList.add(m);
		}
		
		base.createIndex("shaofei", "shaofei", arrayList);
	}
	
	public void mainTest() throws EsExecuteException {
		RecordDao dao = new RecordDao();
//		List<Record> queryRecord = dao.queryRecord("John", "name", 10, 0);
		dao.searchByQuery(Record.class,"John", "name", 10, 0);
	}
}
