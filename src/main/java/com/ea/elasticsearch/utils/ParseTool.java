package com.ea.elasticsearch.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.ea.elasticsearch.model.Host;

/**
 * 字符串解析类
 */
public class ParseTool {

    /**
     * 组装Host
     * @param hosts
     * @return
     */
    public static List<Host> makeUpHost(String hosts) {
    	List<Host> hostList = new ArrayList<>();
    		String[] splitHost = hosts.split(";");
    		for (String host : splitHost) {
				if(StringUtils.isNotBlank(host)) {
					if(host.indexOf(":") != -1) {
						String[] hostSplited = host.split(":");
						if(hostSplited.length == 2) {
							Host hostEntity = new Host();
							String ip = hostSplited[0];
							Integer port = null;
							try {
								port = Integer.parseInt(hostSplited[1]);
							} catch (Exception e) {
								//ignore
							}
							if(port != null && StringUtils.isNotBlank(ip)) {
								hostEntity.setIp(ip);
								hostEntity.setPort(port);
								hostList.add(hostEntity);
							}
						}
					}
				}
			}
    	return hostList;
    }
}
