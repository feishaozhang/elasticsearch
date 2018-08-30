package com.ea.elasticsearch;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.ea.elasticsearch.dao.ElasticSearchOperator;
import com.ea.elasticsearch.model.Record;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppTest2 extends TestCase
{
	private final static Logger logger = Logger.getLogger(ElasticsearchEngine.class);
	@Test
	public void testtest()  {
		ElasticSearchOperator<Record> op = new ElasticSearchOperator<>("carsinfo", "item");
		//查找文档
//		List<Record> queryRecord = dao.queryRecord("John", "name", 10, 1);
//		dao.getDocument("1");
//		User u = new User();
//		u.setName("lisi");
		//添加文档
//		boolean saveDocument = op.saveDocument(u);
		//删除索引
//		op.deleteDocument("1");
		//添加别名
//		op.addAlias("carsitem_v2", "carsinfo");
		//获取文档
//		String document = op.getDocument("1");
//		logger.info(document);
		//获取所有的索引
//		op.getAllIndices();
		//移除别名
//		op.removeAlias("caritems", "carsinfo2");
		op.reIndexDocument("carsitem_v2", "carsitem_v3");
		
	}
	
	class User {
		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
		
		
	}
}
