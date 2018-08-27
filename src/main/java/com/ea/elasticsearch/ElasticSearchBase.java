package com.ea.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.ea.elasticsearch.exception.EsExecuteException;
import com.ea.elasticsearch.model.MappingModel;
import com.ea.elasticsearch.model.Record;
import com.ea.elasticsearch.utils.GenerateTool;
import com.ea.elasticsearch.utils.ResourceUtils;
import com.google.gson.Gson;

/**
 * ES 基础类
 */
public class ElasticSearchBase {

	private static Logger logger = Logger.getLogger(ElasticSearchBase.class);

	protected TransportClient esClient;
	protected String index;
	protected String type;
	protected boolean isSecurity = true;
	//默认分词器
	private String defaultAnalyzer = ResourceUtils.getProperty("defaultAnalyzer");
	//默认搜索方式
	private SearchType searchType = SearchType.DFS_QUERY_THEN_FETCH;// 这种搜索方式准确率高

	public ElasticSearchBase(String index, String type) {
		this.esClient = ElasticsearchEngine.getInstance(isSecurity);
		this.type = type;
		this.index = index;
	}

//	public ElasticSearchBase() {
//		this.esClient = ElasticsearchEngine.getInstance(isSecurity);
//	}
	
	/**
	 * 创建索引
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型
	 * @param mappingModelList
	 *            映射属性列表
	 * @return
	 */
	protected boolean createIndex(String index, String type, List<MappingModel> mappingModelList)
			throws EsExecuteException {
		if (StringUtils.isBlank(index) || StringUtils.isBlank(type)) {
			throw new EsExecuteException("参数异常");
		}
		try {
			// 校验创建的索引名称是否存在
			if (esClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists()) {
				return false;
			}
			// 创建索引
			CreateIndexResponse actionGet = esClient.admin().indices().prepareCreate(index).execute().actionGet();
			boolean acknowledged = actionGet.isAcknowledged();
			if (!acknowledged) {// 生成失败
				logger.error("生成索引失败");

				return false;
			}
		} catch (Exception e) {
			throw new EsExecuteException("生成索引失败 " + e.getLocalizedMessage());
		}

		try {
			XContentBuilder mapping = null;
			mapping = GenerateTool.generateMappingModel(mappingModelList);
			PutMappingRequest paramPutMappingRequest = Requests.putMappingRequest(index);
			if (StringUtils.isNotBlank(type)) {
				paramPutMappingRequest = paramPutMappingRequest.type(type);
			}
			paramPutMappingRequest = paramPutMappingRequest.source(mapping);
			PutMappingResponse putMappingResponse = esClient.admin().indices().putMapping(paramPutMappingRequest)
					.actionGet();
			if (!putMappingResponse.isAcknowledged()) {// 设置索引失败仍然会返回,仅打印出异常
				logger.error("生成映射失败");
			}
		} catch (Exception e) {
			logger.error("生成映射失败");
		}

		return true;
	}

	/**
	 * 索引文档，也就是保存文档到ES
	 * 
	 * @param document
	 * @return
	 */
	public boolean saveDocument(Object document) throws EsExecuteException {
		if(document == null) {
			throw new EsExecuteException("document 不能为空");
		}
		
		Gson g = new Gson();
		IndexResponse indexResponse = null;
		try {
			indexResponse = esClient.prepareIndex(index, type).setSource(g.toJson(document), XContentType.JSON)
					.execute().actionGet();
			return indexResponse.getResult() == UpdateResponse.Result.CREATED;
		} catch (Exception e) {
			logger.error("索引文档出错," + e.getLocalizedMessage());
			throw new EsExecuteException("索引文档出错," + e.getLocalizedMessage());
		}
	}

	/**
	 * 根据id获取文档
	 * 
	 * @param id
	 *            文档ID
	 * @return 返回JSON格式文档
	 */
	public String getDocument(String id) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			return null;
		}
		GetResponse getResponse = null;
		try {
			GetRequestBuilder prepareGet = esClient.prepareGet(index, type, id);
			System.out.println(prepareGet.request().toString());
			getResponse = esClient.prepareGet(index, type, id).execute().actionGet();
			return getResponse.getSourceAsString();
		} catch (Exception e) {
			logger.error("获取文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
	}
	
	/**
	 * 根据id获取文档,泛型
	 * 
	 * @param id
	 *            文档ID
	 * @return 返回JSON格式文档
	 */
	public <T> T getDocument(String id,Class<T> t) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			return null;
		}
		try {
			String sourceAsString = getDocument(id);
			if(StringUtils.isNotBlank(sourceAsString)) {
				Gson g = new Gson();
				T result = g.fromJson(sourceAsString, t);
				return result;
			}
		} catch (Exception e) {
			logger.error("获取文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
		return null;
	}

	/**
	 * 根据id获取文档
	 * 
	 * @param id
	 *            文档ID
	 * @return 返回JSON格式文档
	 */
	public boolean deleteDocument(String id) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			throw new EsExecuteException("参数异常");
		}
		DeleteResponse response = null;
		try {
			response = esClient.prepareDelete(index, type, id).execute().actionGet();
			return response.getResult() == UpdateResponse.Result.DELETED;
		} catch (Exception e) {
			logger.error("获取文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
	}

	/**
	 * 更新文档
	 * 
	 * @param id
	 * @param document
	 * @return
	 */
	public boolean updateDocument(String id, Object document) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			throw new EsExecuteException("参数异常");
		}
		
		UpdateRequest updateRequest = new UpdateRequest();
		Gson gson = new Gson();
		UpdateResponse response = null;
		try {
			String documentJson = gson.toJson(document);
			updateRequest.index(index);
			updateRequest.type(type);
			updateRequest.id(id);
			updateRequest.doc(gson.toJson(documentJson), XContentType.JSON);
			response = esClient.update(updateRequest).get();
			return response.getResult() == UpdateResponse.Result.UPDATED;
		} catch (Exception e) {
			logger.error(e);
			throw new EsExecuteException(e.getMessage());
		}
	}

	/**
	 * 搜索es库中文章
	 *
	 * @param query
	 *            关键词
	 * @param size
	 *            页面容量
	 * @param page
	 *            页码
	 * @return
	 * @throws EsExecuteException
	 */
	public SearchHits searchByQuery(String query, String field, int size, int page) throws EsExecuteException {
		if(StringUtils.isBlank(query) || size <= 0 || page <= 0 ) {
			throw new EsExecuteException("参数异常");
		}
		
		try {
			SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type)
					.setQuery(QueryBuilders.queryStringQuery(query).analyzer(defaultAnalyzer)// 默认分词器
					.defaultField(field))// 检索字段
					.setSize(size).setFrom(size * (page - 1)).setSearchType(searchType);
			SearchResponse response = searchRequestBuilder.execute().actionGet();
			return response.getHits();
		} catch (Exception e) {
			throw new EsExecuteException("搜索失败" + e.getLocalizedMessage());
		}
	}
	
	/**
	 * 搜索es库中文章， 泛型
	 *
	 * @param query
	 *            关键词
	 * @param size
	 *            页面容量
	 * @param page
	 *            页码
	 * @return 
	 * @return
	 * @throws EsExecuteException
	 */
	public <T> List<T> searchByQuery(Class<T> t, String query, String field, int size, int page) throws EsExecuteException {
		if(StringUtils.isBlank(query) || size <= 0 || page <= 0 ) {
			throw new EsExecuteException("参数异常");
		}
		
		try {
			Gson g = new Gson();
			List<T> recordList = new ArrayList<>();
			SearchHits searchByQuery = searchByQuery(query, field, size, page);
			for (SearchHit searchHit : searchByQuery) {
				String sourceAsString = searchHit.getSourceAsString();
				T fromJson = g.fromJson(sourceAsString, t);
				recordList.add(fromJson);
			}
			return recordList;
		} catch (Exception e) {
			throw new EsExecuteException("搜索失败" + e.getLocalizedMessage());
		}
	}

}
