package com.tchaicatkovsky.lock.example;

import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tchaicatkovsky.lock.LSClient;
import com.tchaicatkovsky.lock.NodeInfo;
import com.tchaicatkovsky.pax.common.Config;

public class Client04 {
	private static Logger logger = LoggerFactory.getLogger(Client04.class);
	
	public void buildTree(LSClient lsc) throws Exception {
		logger.info("[{}] start build tree", lsc.uuid());
		
		lsc.createNode("/ls", null);
		lsc.createNode("/ls/local", null);
		lsc.createNode("/ls/local/bigtable", null);
		lsc.createNode("/ls/local/bigtable/master", null);
		lsc.createNode("/ls/local/bigtable/tablets", null);
	}
	
	public void travelTree(LSClient lsc) throws Exception {
		logger.info("[{}] start travel tree", lsc.uuid());
		LinkedList<String> pathList = new LinkedList<>();
		pathList.add("/");
		while (pathList.size() > 0) {
			String path = pathList.pollFirst();
			NodeInfo ni = lsc.queryNodeChildren(path);
			logger.info("[{}] query result: path={}, children={}", lsc.uuid(), path, ni.childrenName);
			for (String name : ni.childrenName) {
				if (path.equals("/"))
					pathList.add(path+name);
				else 
					pathList.add(path+"/"+name);
			}
		}
	}
	
	public void run() throws Exception {
		Config baseConfig = new Config()
				.setUuid(90)
				.setPeers("1:localhost:42221,2:localhost:42222,3:localhost:42223")
				.setName("client");
		
		LSClient lsc = new LSClient(baseConfig);
		lsc.start();
		
		logger.info("[{}] start success, uuid={}", lsc.uuid(), lsc.uuid());

		buildTree(lsc);
		
		Thread.sleep(1000);
				
		travelTree(lsc);
		
		Thread.sleep(1000);
		
		lsc.stop();
	}
	
	public static void main(String args[]) throws Exception {
		Client04 c = new Client04();
		c.run();
	}
} 
