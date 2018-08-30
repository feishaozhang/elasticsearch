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
		ElasticSearchBase<Record> base = new ElasticSearchBase<Record>("carsitem_v3","item");
		//映射属性集
		ArrayList<MappingModel> arrayList = new ArrayList<MappingModel>();
		
		MappingModel m = new MappingModel();
		m.setFieldName("carname");
		HashMap<String, String> hashMap = new HashMap<String, String>();
		hashMap.put(EnumMappingProperties.TYPE.getName(), EnumEsDataType.TEXT.getName());
		m.setProperties(hashMap);
		arrayList.add(m);
		
		
		MappingModel m2 = new MappingModel();
		m2.setFieldName("price");
		HashMap<String, String> hashMap2 = new HashMap<String, String>();
		hashMap2.put(EnumMappingProperties.TYPE.getName(), EnumEsDataType.DOUBLE.getName());
		m2.setProperties(hashMap2);
		arrayList.add(m2);
		
		MappingModel m3 = new MappingModel();
		m3.setFieldName("color");
		HashMap<String, String> hashMap3 = new HashMap<String, String>();
		hashMap3.put(EnumMappingProperties.TYPE.getName(), EnumEsDataType.TEXT.getName());
		m3.setProperties(hashMap3);
		arrayList.add(m3);
		
		MappingModel m4 = new MappingModel();
		m4.setFieldName("date");
		HashMap<String, String> hashMap4 = new HashMap<String, String>();
		hashMap4.put(EnumMappingProperties.TYPE.getName(), EnumEsDataType.DATE.getName());
		m4.setProperties(hashMap4);
		arrayList.add(m4);
		
		boolean createIndex = base.createIndex("carsitem_v3","item", arrayList);
		System.out.println(createIndex);
	}
	
//	public void mainTest() throws EsExecuteException {
//		RecordDao dao = new RecordDao();
////		List<Record> queryRecord = dao.queryRecord("John", "name", 10, 0);
//		dao.searchByQuery(Record.class,"John", "name", 10, 0);
//	}
}
