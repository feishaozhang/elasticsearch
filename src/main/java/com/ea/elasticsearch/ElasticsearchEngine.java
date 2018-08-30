package com.ea.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import com.ea.elasticsearch.model.Host;
import com.ea.elasticsearch.utils.ParseTool;
import com.ea.elasticsearch.utils.ResourceUtils;

/**
 * Elastic search Java 客户端
 */
public class ElasticsearchEngine {
	//预留字段，判断当前是否是安全验证
	private static volatile boolean isSecurity = false;
	private final static Logger logger = Logger.getLogger(ElasticsearchEngine.class);

	//transport客户端
    private static volatile Client client;
    private static PreBuiltTransportClient preBuiltTransportClient = null;
    
    public ElasticsearchEngine() {
    }
    
    public static Client getInstance(boolean isSecurity) throws ElasticsearchException{
    	if(client == null) {
    		synchronized (ElasticsearchEngine.class) {
    			if(client == null) {
    				initElasticSearch(isSecurity);
    				ElasticsearchEngine.isSecurity = isSecurity;
    			}
			}
    	}
    	return client;
    }
    
    /**
     * 初始化连接
     * @param isSecurity
     */
    @SuppressWarnings("resource")
	public static void initElasticSearch(boolean isSecurity)throws ElasticsearchException {
    	//主机地址
    	String host = ResourceUtils.getProperty("clientHost");
        //集群名称
    	String esClusterName = ResourceUtils.getProperty("esClusterName");
        
    	//检查IP，端口
    	if(StringUtils.isBlank(host)) {
    		throw new ElasticsearchException("请配置Host以ip:port方式配置，如果有多个host则用';'隔开");
    	}
    	
    	if(StringUtils.isBlank(esClusterName)) {
    		throw new ElasticsearchException("请配置esClusterName集群名称");
    	}
    	
    	List<Host> makeUpHost = ParseTool.makeUpHost(host);
    	if(makeUpHost == null || makeUpHost.size() == 0) {
    		throw new ElasticsearchException("请配置Host以ip:port方式配置，如果有多个host则用';'隔开");
    	}
    	
    	//创建客户端
        try {
        	Settings settings = Settings.EMPTY;
        	
        	if(isSecurity) {
        		String esUserName = ResourceUtils.getProperty("esUserName");
        		String esPassword = ResourceUtils.getProperty("esPassword");
        		if(StringUtils.isBlank(esUserName) || StringUtils.isBlank(esPassword)) {
        			throw new ElasticsearchException("请配置ES用户名密码");
        		}
        		settings = Settings.builder()
    				.put("cluster.name", esClusterName)
    				.put("client.transport.sniff", false)
    	            .put("xpack.security.transport.ssl.enabled", false)
    				.put("xpack.security.user", esUserName+":"+esPassword)
    				.build();
        		preBuiltTransportClient = new PreBuiltXPackTransportClient(settings);
        	}else {
        		settings = Settings.builder()
        				.put("cluster.name", esClusterName)
        				.build();
        		preBuiltTransportClient = new PreBuiltTransportClient(settings);
        	}
        	
        	//设置远程连接一个elasticsearch集群，TransportClient 会通过轮询的方法与这些地址进行通信
        	for (Host host2 : makeUpHost) {
        		client = preBuiltTransportClient.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(host2.getIp()),host2.getPort()));
			}
			
			final ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth()
					.setTimeout(TimeValue.timeValueMinutes(1)).execute().actionGet();
			if (healthResponse.isTimedOut()) {
				client = null;
				logger.error("Elastic 客户端连接失败");
			} else {
				logger.info("ES客户端初始化成功"+"集群名称："+healthResponse.getClusterName());
			}
		} catch (UnknownHostException e) {
			throw new ElasticsearchException("Elastic 客户端连接失败");
		}
    }
    

    
    public static void closeClient() {
    	try {
    		if(client != null) {
    			client.close();
    			client = null;
    		}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
    	
		try {
			if (preBuiltTransportClient != null) {
				preBuiltTransportClient.close();
				preBuiltTransportClient = null;
			}
		} catch (Exception e2) {
			logger.error("关闭preBuiltTransportClient失败: " + e2.getMessage());
		}
    }
    

    
    public static void main(String[] args) {
		
	}
}