package com.ea.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import com.ea.elasticsearch.en.EnumBulkCode;
import com.ea.elasticsearch.exception.EsExecuteException;
import com.ea.elasticsearch.model.BulkResult;
import com.ea.elasticsearch.model.MappingModel;
import com.ea.elasticsearch.utils.GenerateTool;
import com.ea.elasticsearch.utils.ObjectUtil;
import com.ea.elasticsearch.utils.ResourceUtils;


/**
 * <p>ES 基础类，提供主要访问ES的方法</p>
 */
public class ElasticSearchBase<T> {
	private final static Logger logger = Logger.getLogger(ElasticSearchBase.class);
	//ES客户端
	protected Client esClient;
	//索引名称
	protected String index;
	//索引-类型名称
	protected String type;
	//是否是安全连接
	protected boolean isSecurity = true;
	//默认分词器
	private String defaultAnalyzer = "ik_smart";
	//默认搜索方式
	private SearchType searchType = SearchType.QUERY_THEN_FETCH;// 这种搜索方式准确率高

	public ElasticSearchBase(String index, String type) {
		this.esClient = ElasticsearchEngine.getInstance(isSecurity);
		this.type = type;
		this.index = index;
		init();
	}
	
	/**
	 * 初始化参数
	 */
	private void init() {
		//配置文件的分词器
		String analyzer = ResourceUtils.getProperty("defaultAnalyzer");
		if(StringUtils.isNotBlank(analyzer)) {
			defaultAnalyzer = analyzer;
		}
		
		String searchTypeProperty = ResourceUtils.getProperty("searchType");
		//搜索类型
		SearchType searchTypeInProFile = SearchType.valueOf(searchTypeProperty);
		if(searchTypeInProFile != null) {
			searchType = searchTypeInProFile;
		}
	}
	
	/**
	 * 校验索引是否存在
	 * @param index 索引名称
	 * @return true 存在 false 不存在
	 * @throws EsExecuteException
	 */
	protected boolean isIndexExist(String index)throws EsExecuteException {
		if (StringUtils.isBlank(index) || StringUtils.isBlank(type) ) {
			throw new EsExecuteException("参数异常");
		}
		
		try {
			return esClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
		}catch (Exception e) {
			throw new ElasticsearchException("");
		}
	}
	
	/**
	 * 创建索引-并根据mappingModelList 生成映射
	 * @param index 索引名称
	 * @param type 类型
	 * @param mappingModelList 映射属性列表  
	 * {@link MappingModel}  fieldName 字段名称，properties 字段属性，如设置分词器等
	 * @return true创建索引成功 false 创建索引失败
	 */
	protected boolean createIndex(String index, String type, List<MappingModel> mappingModelList)
			throws EsExecuteException {
		if (StringUtils.isBlank(index) || StringUtils.isBlank(type) ) {
			throw new EsExecuteException("参数异常");
		}
		
		String tagName = "indexName ="+index;
		try {
			// 校验创建的索引名称是否存在
			if (isIndexExist(index)) {
				throw new EsExecuteException("该索引已经存在"+tagName);
			}
			
			CreateIndexRequestBuilder prepareCreate = esClient.admin().indices().prepareCreate(index);
			Settings createIndexSetting = GenerateTool.createIndexSetting();
			//设置分片跟backup数量，如果配置没有配置，则使用ES默认设置
			if(createIndexSetting != null) {
				prepareCreate.setSettings(createIndexSetting);
			}
			
			// 创建索引
			CreateIndexResponse actionGet = prepareCreate.execute().actionGet();
			boolean acknowledged = actionGet.isAcknowledged();
			if (!acknowledged) {// 生成失败
				logger.error("生成索引失败"+tagName);
				return false;
			}
		} catch (Exception e) {
			logger.error("创建索引失败"+e);
			throw new EsExecuteException("生成索引失败 "+tagName + e.getLocalizedMessage());
		}

		//配置映射
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
				logger.error("生成映射失败"+tagName);
			}
		} catch (Exception e) {
			logger.error("生成映射失败"+tagName+e);
		}

		return true;
	}

	/**
	 * 也就是保存文档到ES
	 * @param document 文档
	 * @return true成功 false失败
	 */
	public boolean saveDocument(Object document) throws EsExecuteException {
		if(document == null) {
			logger.error("document 不能为空");
			throw new EsExecuteException("document 不能为空");
		}
		
		try {
			IndexResponse indexResponse = null;
			String jsonDocument;
			if(document instanceof String) {
				jsonDocument = String.valueOf(document);
			}else {
				jsonDocument = ObjectUtil.objectToJsonWithXStream(document);
			}
			 
			indexResponse = esClient.prepareIndex(index, type).setSource(jsonDocument, XContentType.JSON)
					.execute().actionGet();
			return indexResponse.getResult() == UpdateResponse.Result.CREATED;
		} catch (Exception e) {
			logger.error("索引文档出错," + e.getLocalizedMessage());
			throw new EsExecuteException("索引文档出错," + e.getLocalizedMessage());
		}
	}

	/**
	 * 根据文档id获取文档
	 * @param id 文档ID
	 * @return 返回JSON格式文档
	 */
	public String getDocument(String id) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			return null;
		}
		try {
			GetResponse getResponse = null;
			getResponse = esClient.prepareGet(index, type, id).execute().actionGet();
			return getResponse.getSourceAsString();
		} catch (Exception e) {
			logger.error("获取文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
	}
	
	/**
	 * 
	 *根据id获取文档,泛型
	 * @param id文档ID
	 * @return 返回JSON格式文档
	 * @throws EsExecuteException
	 */
	public T getDocument(String id,Class<T> t) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			return null;
		}
		try {
			String sourceAsString = getDocument(id);
			if(StringUtils.isNotBlank(sourceAsString)) {
				T result = ObjectUtil.jsonToObjectWithJackson(sourceAsString, t);
				return result;
			}
		} catch (Exception e) {
			logger.error("获取文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
		return null;
	}

	/**
	 * 根据id删除文档-如果参数异常会抛出异常
	 * @param id 文档ID
	 * @return 返回JSON格式文档
	 */
	public boolean deleteDocument(String id) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			throw new EsExecuteException("参数异常");
		}
		
		try {
			DeleteResponse response = null;
			response = esClient.prepareDelete(index, type, id).execute().actionGet();
			return response.getResult() == UpdateResponse.Result.DELETED;
		} catch (Exception e) {
			logger.error("删除文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("删除文档失败," + e.getLocalizedMessage());
		}
	}

	/**
	 * 更新文档
	 * @param id 文档Id
	 * @param document 文档对象
	 * @return
	 */
	public boolean updateDocument(String id, Object document) throws EsExecuteException {
		if(StringUtils.isBlank(id)) {
			throw new EsExecuteException("参数异常");
		}
		
		try {
			UpdateResponse response = null;
			UpdateRequest updateRequest = new UpdateRequest();
			String jsonDocument;
			if(document instanceof String) {
				jsonDocument = String.valueOf(document);
			}else {
				jsonDocument = ObjectUtil.objectToJsonWithXStream(document);
			}
			updateRequest.index(index);
			updateRequest.type(type);
			updateRequest.id(id);
			updateRequest.doc(jsonDocument, XContentType.JSON);
			response = esClient.update(updateRequest).get();
			return response.getResult() == UpdateResponse.Result.UPDATED;
		} catch (Exception e) {
			logger.error(e);
			throw new EsExecuteException(e.getMessage());
		}
	}

	/**
	 * 搜索es库中文章
	 * @param query 关键词
	 * @param size 页面容量
	 * @param page  页码
	 * @return SearchHits
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
	 * 搜索es库中文章
	 * @param query 关键词
	 * @param size 页面容量
	 * @param page 页码
	 * @return
	 * @throws EsExecuteException
	 */
	public SearchHits searchByQueryComplex(String query, String field, int size, int page) throws EsExecuteException {
		if(StringUtils.isBlank(query) || size <= 0 || page <= 0 ) {
			throw new EsExecuteException("参数异常");
		}
		
		try {
			SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type)
					.setQuery(QueryBuilders.queryStringQuery(query).analyzer(defaultAnalyzer)// 默认分词器
					.defaultField(field))// 检索字段
					.setSize(size).setFrom(size * (page - 1)).setSearchType(searchType)
					.setPostFilter(QueryBuilders.rangeQuery("age").gt(30).lt(40))
					.addSort("sortField", SortOrder.DESC);
				 	
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
	public List<T> searchByQuery(Class<T> t, String query, String field, int size, int page) throws EsExecuteException {
		if(StringUtils.isBlank(query) || size <= 0 || page < 0 ) {
			throw new EsExecuteException("参数异常");
		}
		
		try {
			List<T> recordList = new ArrayList<>();
			SearchHits searchByQuery = searchByQuery(query, field, size, page);
			for (SearchHit searchHit : searchByQuery) {
				String sourceAsString = searchHit.getSourceAsString();
				T jsonToObjectWithJackson = ObjectUtil.jsonToObjectWithJackson(sourceAsString, t);
				recordList.add(jsonToObjectWithJackson);
			}
			return recordList;
		} catch (Exception e) {
			throw new EsExecuteException("搜索失败" + e.getLocalizedMessage());
		}
	}
	
	/**
	 * 根据多个 id删除文档-伪事务(由于ES不支持事务操作)-同时失败或者同时成功，弱事务
	 * @param ids 多个文档文档ID
	 * @param isNeedTransaction 是否需要事务支持
	 * @return 返回JSON格式文档
	 */
	public BulkResult deleteDocumentBulk(List<String> ids, boolean isNeedTransaction) throws EsExecuteException {
		
		if(ids == null || ids.size() == 0) {
			throw new EsExecuteException("参数异常");
		}
		
		BulkResult result = new BulkResult();
		try {
			List<String> documentBulk = new ArrayList<>();//用于回滚数据
			BulkRequestBuilder bulkRequest = esClient.prepareBulk();
			if(isNeedTransaction) {
				documentBulk.addAll(getDocumentBulk(ids));
			}
			
			for (String id : ids) {
				DeleteRequestBuilder prepareDelete = esClient.prepareDelete(index, type, id);
				bulkRequest.add(prepareDelete);
			}
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			
			if(!bulkResponse.hasFailures()) {
				result.setCode(EnumBulkCode.SUCCESS.getCode());
				result.setMsg("批量操作成功");
				return result;
			}
			
			if(bulkResponse.hasFailures()) {
				if(isNeedTransaction) {
					for (BulkItemResponse bulkItemResponse : bulkResponse) {
						if(!bulkItemResponse.isFailed()) {//删除失败，对操作成功的数据进行数据回滚，。
							saveDocumentBulkHJson(documentBulk, false);
						}
					}
					result.setCode(EnumBulkCode.FAILD.getCode());
					result.setMsg(EnumBulkCode.FAILD.getMsg());
				}else {
					result.setCode(EnumBulkCode.COPLETE.getCode());
					result.setMsg(EnumBulkCode.COPLETE.getMsg());
				}
			}
		} catch (Exception e) {
			logger.error("获取文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
		return result;
	}
	
	/**
	 * 索引文档，也就是保存文档到ES-批量
	 * @param document
	 * @param isNeedTransaction 伪事务支持
	 * @return
	 */
	public BulkResult saveDocumentBulkObject(List<Object> document, boolean isNeedTransaction) throws EsExecuteException {
		if(document == null) {
			throw new EsExecuteException("document 不能为空");
		}
		
		BulkResult result = new BulkResult();
		
		try {
			BulkRequestBuilder bulkRequest = esClient.prepareBulk();
			for (Object object : document) {
				String jsonDocument = ObjectUtil.objectToJsonWithXStream(object);
				IndexRequestBuilder setSource = esClient.prepareIndex(index, type).setSource(jsonDocument, XContentType.JSON);
				bulkRequest.add(setSource);
			}
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			
			if(!bulkResponse.hasFailures()) {
				result.setCode(EnumBulkCode.SUCCESS.getCode());
				result.setMsg(EnumBulkCode.SUCCESS.getMsg());
				return result;
			}
			
			if(isNeedTransaction) {//伪事务支持
				if(bulkResponse.hasFailures()) {
					List<String> rollbackIds = new ArrayList<>();
					BulkItemResponse[] items = bulkResponse.getItems();
					for (BulkItemResponse bulkItemResponse : items) {
						if(!bulkItemResponse.isFailed()) {//数据回滚
							rollbackIds.add(bulkItemResponse.getId());
						}
					}
					deleteDocumentBulk(rollbackIds, false);
					result.setCode(EnumBulkCode.FAILD.getCode());
					result.setMsg(EnumBulkCode.FAILD.getMsg());
				}
			}else {
				result.setCode(EnumBulkCode.COPLETE.getCode());
				result.setMsg(EnumBulkCode.COPLETE.getMsg());
			}
			return result;
		} catch (Exception e) {
			logger.error("索引文档出错," + e.getLocalizedMessage());
			throw new EsExecuteException("索引文档出错," + e.getLocalizedMessage());
		}
	}
	
	/**
	 * 索引文档，也就是保存文档到ES-批量
	 * @param document
	 * @param isNeedTransaction 伪事务支持
	 * @return
	 */
	public BulkResult saveDocumentBulkHJson(List<String> document, boolean isNeedTransaction) throws EsExecuteException {
		
		if(document == null || document.size() == 0) {
			throw new EsExecuteException("document 不能为空");
		}
		
		BulkResult result = new BulkResult();
		try {
			BulkRequestBuilder bulkRequest = esClient.prepareBulk();
			for (String object : document) {
				IndexRequestBuilder setSource = esClient.prepareIndex(index, type).setSource(object, XContentType.JSON);
				bulkRequest.add(setSource);
			}
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			
			if(!bulkResponse.hasFailures()) {
				result.setCode(EnumBulkCode.SUCCESS.getCode());
				result.setMsg(EnumBulkCode.SUCCESS.getMsg());
				return result;
			}
			if(isNeedTransaction) {
				List<String> rollbackIds = new ArrayList<>();
				BulkItemResponse[] items = bulkResponse.getItems();
				for (BulkItemResponse bulkItemResponse : items) {
					if(!bulkItemResponse.isFailed()) {
						rollbackIds.add(bulkItemResponse.getId());
					}
				}
				result.setCode(EnumBulkCode.FAILD.getCode());
				result.setMsg(EnumBulkCode.FAILD.getMsg());
			}else {
				result.setCode(EnumBulkCode.COPLETE.getCode());
				result.setMsg(EnumBulkCode.COPLETE.getMsg());
			}
			return result;
		} catch (Exception e) {
			logger.error("索引文档出错," + e.getLocalizedMessage());
			throw new EsExecuteException("索引文档出错," + e.getLocalizedMessage());
		}
	}
	
	/**
	 * 根据id获取文档,泛型-批量
	 * @param id文档ID
	 * @return 返回JSON格式文档
	 */
	public List<T> getDocumentBulk(List<String> ids, Class<T> t) throws EsExecuteException {
		if(ids == null || ids.size()==0) {
			throw new EsExecuteException("参数异常");
		}
		
		List<T> responseList = new ArrayList<>();
		try {
			MultiGetRequestBuilder prepareGet = esClient.prepareMultiGet();
			for (String id : ids) {
				prepareGet.add(index, type, id);
			}
			MultiGetResponse multiGetResponse = prepareGet.get();
			for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
				GetResponse response = multiGetItemResponse.getResponse();
	            if (response.isExists()) {
	                String sourceAsString = response.getSourceAsString();
	                if(StringUtils.isNotBlank(sourceAsString)) {
	                	T result = ObjectUtil.jsonToObjectWithJackson(sourceAsString, t);
	                	responseList.add(result);
	                }
	            }
	            
			}
		} catch (Exception e) {
			logger.error("获取文档失败," + e);
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
		return responseList;
	}
	
	
	/**
	 * 根据id获取文档JSON数组
	 * @param id文档ID
	 * @return 返回JSON格式文档
	 */
	public List<String> getDocumentBulk(List<String> ids) throws EsExecuteException {
		if(ids == null || ids.size()==0) {
			throw new EsExecuteException("参数异常");
		}
		
		List<String> responseList = new ArrayList<>();
		try {
			MultiGetRequestBuilder prepareGet = esClient.prepareMultiGet();
			for (String id : ids) {
				prepareGet.add(index, type, id);
			}
			MultiGetResponse multiGetResponse = prepareGet.get();
			for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
				GetResponse response = multiGetItemResponse.getResponse();
	            if (response.isExists()) {
	                String sourceAsString = response.getSourceAsString();
	                if(StringUtils.isNotBlank(sourceAsString)) {
	                	responseList.add(sourceAsString);
	                }
	            }
			}
		} catch (Exception e) {
			logger.error("获取文档失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取文档失败," + e.getLocalizedMessage());
		}
		return responseList;
	}
	
	/**
	 * 更新文档-bulk-批量
	 * @param dataMap 数据组
	 * @param isNeedTransaction 是否需要事务支持，伪事务用于保持方法的原子性，并不能保证绝对的原子性
	 * @return
	 */
	public BulkResult updateDocumentBulk(Map<String, Object> dataMap, boolean isNeedTransaction) throws EsExecuteException {
		
		if(dataMap == null || dataMap.size() == 0) {
			throw new EsExecuteException("参数异常");
		}
		
		BulkResult result = new BulkResult();
		try {
			UpdateRequest updateRequest = new UpdateRequest();
			Iterator<Entry<String, Object>> iterator = dataMap.entrySet().iterator();
			Map<String,String> documentBulk = new HashMap<>();
			if(isNeedTransaction) {//如果需要事务需要先获取修改的所有内容
				Set<String> keySet = dataMap.keySet();
				if(keySet != null && keySet.size() > 0) {
					Iterator<String> iterator2 = keySet.iterator();
					while(iterator2.hasNext()) {
						String next = iterator2.next();
						String document = getDocument(next);
						documentBulk.put(next, document);
					}
				}
			}
			
			BulkRequestBuilder bulkRequest = esClient.prepareBulk();
			while(iterator.hasNext()) {
				Entry<String, Object> next = iterator.next();
				String id = next.getKey();
				Object source = next.getValue();
				String jsonDocument = String.valueOf(source);
				if(!(source instanceof String)) {
					jsonDocument = ObjectUtil.objectToJsonWithJackson(source);
				}
				updateRequest.index(index);
				updateRequest.type(type);
				updateRequest.id(id);
				updateRequest.doc(jsonDocument, XContentType.JSON);
				bulkRequest.add(updateRequest);
			}
			
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if(!bulkResponse.hasFailures()){
				result.setCode(EnumBulkCode.SUCCESS.getCode());
				result.setMsg(EnumBulkCode.SUCCESS.getMsg());
				return result;
			}
			
			if(isNeedTransaction) {
				for (BulkItemResponse bulkItemResponse : bulkResponse) {
					if(!bulkItemResponse.isFailed()) {
						String id = bulkItemResponse.getId();
						String string = documentBulk.get(id);
						updateDocument(id, string);
					}
				}
				result.setCode(EnumBulkCode.FAILD.getCode());
				result.setMsg(EnumBulkCode.FAILD.getMsg());
			}else {
				result.setCode(EnumBulkCode.COPLETE.getCode());
				result.setMsg(EnumBulkCode.COPLETE.getMsg());
			}
		} catch (Exception e) {
			logger.error(e);
			throw new EsExecuteException(e.getMessage());
		}
		return result;
	}
	
	/**
	 * 给索引添加别名
	 * @param indexName 索引名称
	 * @param aliasName 别名
	 * @return
	 * @throws EsExecuteException
	 */
	public boolean addAlias(String indexName, String aliasName) throws EsExecuteException {
		
		if(StringUtils.isBlank(indexName) || StringUtils.isBlank(aliasName)) {
			return false;
		}
		try {
			 if(isExsitAlias(indexName, aliasName)) {
				 logger.error("该别名已经存在,添加别名失败,");
				 throw new EsExecuteException("该别名已经存在,添加别名失败,");
			 }
			 
			 if(isIndexExist(indexName)) {
				 logger.error("该索引已经存在,添加别名失败,");
				 throw new EsExecuteException("该索引已经存在,添加别名失败,");
			 }
			 
			 IndicesAliasesRequestBuilder addAlias = esClient.admin().indices().prepareAliases().addAlias(indexName,aliasName);
			 IndicesAliasesResponse actionGet = addAlias.execute().actionGet();
			 if(actionGet.isAcknowledged()) {
				 return true;	
			 }
		} catch (Exception e) {
			logger.error("创建别名失败," + e.getLocalizedMessage());
			throw new EsExecuteException("创建别名失败," + e.getLocalizedMessage());
		}
		return false;
	}
	
	/**
	 * 删除别名
	 * @param indexName 索引名称
	 * @param aliasName 别名
	 * @return
	 * @throws EsExecuteException
	 */
	public boolean removeAlias(String indexName, String aliasName) throws EsExecuteException {
		
		if(StringUtils.isBlank(indexName) || StringUtils.isBlank(aliasName)) {
			return false;
		}
		
		try {
			 if(isExsitAlias(indexName, aliasName)) {
				 logger.error("该别名已经存在,添加别名失败,");
				 throw new EsExecuteException("该别名已经存在,添加别名失败,");
			 }
			 
			 if(isIndexExist(indexName)) {
				 logger.error("该索引已经存在,添加别名失败,");
				 throw new EsExecuteException("该索引已经存在,添加别名失败,");
			 }
			
			 IndicesAliasesRequestBuilder addAlias = esClient.admin().indices().prepareAliases().removeAlias(indexName,aliasName);
			 IndicesAliasesResponse actionGet = addAlias.execute().actionGet();
			 if(actionGet.isAcknowledged()) {
				 return true;	
			 }
		} catch (Exception e) {
			logger.error("移除别名失败," + e.getLocalizedMessage());
			throw new EsExecuteException("移除别名失败," + e.getLocalizedMessage());
		}
		return false;
	}
	
	/**
	 * 判断别名是否存在
	 * @param indexName 索引名称
	 * @param aliasName 别名
	 * @return
	 * @throws EsExecuteException
	 */
	public boolean isExsitAlias(String indexName, String aliasName) throws EsExecuteException {
		if(StringUtils.isBlank(indexName) || StringUtils.isBlank(aliasName)) {
			return false;
		}
		try {
			 AliasesExistRequestBuilder prepareAliasesExist = esClient.admin().indices().prepareAliasesExist(aliasName,indexName);
			 AliasesExistResponse actionGet = prepareAliasesExist.execute().actionGet();
			 if(actionGet.isExists()) {
				 return true;	
			 }
		} catch (Exception e) {
			logger.error("获取别名失败," + e.getLocalizedMessage());
			throw new EsExecuteException("获取别名失败," + e.getLocalizedMessage());
		}
		return false;
	}
	
	/**
	 * 获取所有index
	 */
	public Set<String> getAllIndices() {
	    ActionFuture<IndicesStatsResponse> isr = esClient.admin().indices().stats(new IndicesStatsRequest().all());
	    Set<String> set = isr.actionGet().getIndices().keySet();
	    return set;
	}
	
	/**
	 * 添加别名
	 * @param indexName 索引名称
	 * @param aliasName 别名
	 * @return
	 * @throws EsExecuteException
	 */
	public boolean changeAlias(String indexName, String aliasName) throws EsExecuteException {
		if(StringUtils.isBlank(indexName) || StringUtils.isBlank(aliasName)) {
			return false;
		}
		try {
			 if(isExsitAlias(indexName, aliasName)) {
				 logger.error("该别名已经存在,添加别名失败,");
				 throw new EsExecuteException("该别名已经存在,添加别名失败,");
			 }
			 IndicesAliasesRequestBuilder addAlias = esClient.admin().indices().prepareAliases().addAlias(indexName,aliasName);
			 IndicesAliasesResponse actionGet = addAlias.execute().actionGet();
			 if(actionGet.isAcknowledged()) {
				 return true;	
			 }
		} catch (Exception e) {
			logger.error("创建别名失败," + e.getLocalizedMessage());
			throw new EsExecuteException("创建别名失败," + e.getLocalizedMessage());
		}
		return false;
	}
	
	/**
	 * 重新索引数据-将旧索引上的数据索引到新索引上，当需要修改字段时使用
	 * @param oldIndexName 旧索引名称
	 * @param newIndexName 新索引名称
	 * @return
	 * @throws EsExecuteException
	 */
	public boolean reIndexDocument(String oldIndexName, String newIndexName) throws EsExecuteException {
		if(StringUtils.isBlank(oldIndexName) || StringUtils.isBlank(newIndexName)) {
			throw new ElasticsearchException("参数异常");
		}
		
		try {
			if(!isIndexExist(oldIndexName)) {
				throw new ElasticsearchException("旧索引不存在，请检查后重试");
			}
			
			if(!isIndexExist(newIndexName)) {
				throw new ElasticsearchException("新索引不存在，请检查后重试");
			}
			
			SearchResponse scrollResp = esClient.prepareSearch(oldIndexName) //指定旧索引
				    .setScroll(new TimeValue(60000))
				    .setQuery(QueryBuilders.matchAllQuery()) // 匹配所有
				    .setSize(100).execute().actionGet(); //命中100条数据会返回数据
			
			int BULK_ACTIONS_THRESHOLD = 1000;
			int BULK_CONCURRENT_REQUESTS = 1;
			BulkProcessor bulkProcessor = BulkProcessor.builder(esClient, new BulkProcessor.Listener() {
			    @Override
			    public void beforeBulk(long executionId, BulkRequest request) {
			        logger.info("Bulk Going to execute new bulk composed of {} actions"+request.numberOfActions());
			    }

			    @Override
			    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			        logger.info("Executed bulk composed of {} actions" + request.numberOfActions());
			    }

			    @Override
			    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			        logger.info("Error executing bulk", failure);
			    }
			    }).setBulkActions(BULK_ACTIONS_THRESHOLD).setConcurrentRequests(BULK_CONCURRENT_REQUESTS).setFlushInterval(TimeValue.timeValueMillis(5)).build();
			
			//继续迭代，知道获取了所有数据
			while (true) {
				// Get results from a scan search and add it to bulk ingest
				for (SearchHit hit: scrollResp.getHits()) {
					IndexRequest request = new IndexRequest(newIndexName, hit.type(), hit.id());
					Map source = ((Map) ((Map) hit.getSource()));
					request.source(source);
					bulkProcessor.add(request);
				}
				scrollResp = esClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
			    //退出条件
			    if (scrollResp.getHits().getHits().length == 0) {
			        logger.info("关闭bulk processor");
			        bulkProcessor.close();
			        break; 
			    }
			    return true;
			}
		} catch (Exception e) {
			logger.error("重新索引数据失败," + e.getLocalizedMessage());
			throw new EsExecuteException("重新索引数据失败," + e.getLocalizedMessage());
		}
		return false;
	}
	

}
