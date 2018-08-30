package com.ea.elasticsearch.utils;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;

import com.ea.elasticsearch.ElasticsearchEngine;

/**
 * 获取属性
 *
 */
public class ResourceUtils {
	private final static Logger logger = Logger.getLogger(ElasticsearchEngine.class);
	static Properties prop = new Properties();
	/**
	 * 配置文件目录
	 */
	private static final String ELASTIC_SEARCH_PROPERTIES_PATH = "/elastic.properties";
	
	static {
		InputStream is = null;
		try {
			is = ElasticsearchEngine.class.getClass().getResourceAsStream(ELASTIC_SEARCH_PROPERTIES_PATH);
			prop.load(is);
		} catch (Exception e) {
			logger.error("获取Elastic search 配置文件失败"+e.getMessage());
			throw new ElasticsearchException("加载配置文件出错");
		}finally {
			try {
				if(is != null) {
					is.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	/**
	 * 获取属性
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		if(key == null) {
			return null;
		}
		return prop.getProperty(key);
	}
	
	/**
	 * 获取属性
	 * @param key
	 * @return
	 * @throws Exception 
	 */
	public static Integer getPropertyAsInt(String key) {
		String property = getProperty(key);
		if(StringUtils.isNotBlank(property)) {
			return Integer.parseInt(property);
		}
		return null;
	}
	
}
