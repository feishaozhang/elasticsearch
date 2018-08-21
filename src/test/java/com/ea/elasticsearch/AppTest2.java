package com.ea.elasticsearch;

import org.junit.Test;

import com.ea.elasticsearch.dao.RecordDao;
import com.ea.elasticsearch.exception.EsExecuteException;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppTest2 extends TestCase
{
	
	@Test
	public void testtest()  {
		RecordDao dao = new RecordDao();
//		List<Record> queryRecord = dao.queryRecord("John", "name", 10, 1);
//		dao.getDocument("1");
		User u = new User();
		u.setName("lisi");
		boolean saveDocument = dao.saveDocument(u);
		dao.deleteDocument("1");
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
