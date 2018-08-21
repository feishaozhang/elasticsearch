package com.ea.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import com.ea.elasticsearch.utils.ResourceUtils;

/**
 * Elastic search Java 客户端
 */
public class ElasticsearchEngine {
	//预留字段，判断当前是否是安全验证
	private static volatile boolean isSecurity = false;
	private static Logger logger = Logger.getLogger(ElasticsearchEngine.class);

	//transport客户端
    private static volatile TransportClient client;
    private static PreBuiltTransportClient preBuiltTransportClient = null;
    
    public ElasticsearchEngine() {
    }
    
    public static TransportClient getInstance(boolean isSecurity) {
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
	public static void initElasticSearch(boolean isSecurity) {
    	//主机地址
    	String host = ResourceUtils.getProperty("host");
        //主机端口
    	Integer port = ResourceUtils.getPropertyAsInt("clientPort");
        //集群名称
    	String esClusterName = ResourceUtils.getProperty("esClusterName");
        
    	//检查IP，端口
    	if(StringUtils.isBlank(host)) {
    		logger.error("请配置IP地址");
    		return;
    	}
    	if(port == null) {
    		logger.error("请配置clientPort端口");
    		return;
    	}
    	if(StringUtils.isBlank(esClusterName)) {
    		logger.error("请配置esClusterName集群名称");
    		return;
    	}
    	
    	
    	//创建客户端
        try {
        	Settings settings = Settings.EMPTY;
        	String esUserName = ResourceUtils.getProperty("esUserName");
        	String esPassword = ResourceUtils.getProperty("esPassword");
        	if(StringUtils.isBlank(esUserName) || StringUtils.isBlank(esPassword)) {
        		logger.error("请配置ES用户名密码");
        		return;
        	}
        	
        	if(isSecurity) {
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
        	
			client = preBuiltTransportClient.addTransportAddresses(
                    new InetSocketTransportAddress(InetAddress.getByName(host),port));
			final ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth()
					.setTimeout(TimeValue.timeValueMinutes(1)).execute().actionGet();
			if (healthResponse.isTimedOut()) {
				client = null;
				logger.error("Elastic 客户端连接失败");
			} else {
				
				logger.info("ES客户端初始化成功"+"集群名称："+healthResponse.getClusterName());
			}
		} catch (UnknownHostException e) {
			logger.error("Elastic 客户端连接失败",e);
		}
    }
    
    public static void closeClient() {
    	try {
    		if(client != null) {
    			client.close();
    		}
		} catch (Exception e) {
			logger.error(e);
		}
    	
		try {
			if (preBuiltTransportClient != null) {
				preBuiltTransportClient.close();
			}
		} catch (Exception e2) {
			logger.error("关闭preBuiltTransportClient失败: " + e2.getMessage());
		}
    }
}