/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ht.lock.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ht.lock.Handle;
import ht.lock.HandleFlag;
import ht.lock.LSClient;
import ht.pax.common.Config;
import ht.pax.util.ReflectionUtil;

/**
 * @author Teng Huang ht201509@163.com
 */
public class Client01 {
	private static Logger logger = LoggerFactory.getLogger(Client01.class);
	
	public void buildTree(LSClient lsc) throws Exception {
		logger.info("[{}] start build tree", lsc.uuid());
		
		lsc.createNode("/ls", null);
		lsc.createNode("/ls/local", null);
		lsc.createNode("/ls/local/bigtable", null);
		lsc.createNode("/ls/local/bigtable/master", null);
		lsc.createNode("/ls/local/bigtable/tablets", null);
	}
	
	public void run() throws Exception {
		Config baseConfig = new Config()
				.setUuid(90)
				.setPeers("1:localhost:42221,2:localhost:42222,3:localhost:42223")
				.setName("client");
		
		LSClient lsc = new LSClient(baseConfig);
		lsc.start();
		
		buildTree(lsc);
		
		Throwable t = null;
		try {
			lsc.open("/ls/local/bigtable/master", new byte[]{1,2,3}, HandleFlag.TRY_LOCK | HandleFlag.EPHEMERAL);
		} catch (Exception e) {
			t = e;
		}
		
		logger.info("[{}] open result, e={}", lsc.uuid(), t);
		
		Handle handle = lsc.open("/ls/local/bigtable/master", new byte[]{1,2,3}, HandleFlag.TRY_LOCK);
		byte[] writeData = new byte[]{2,3,4};
		handle.write(writeData);
		byte[] readData = handle.read();
		
		logger.info("[{}] (write.data == read.data) is {}", lsc.uuid(), ReflectionUtil.isEquals(writeData, readData));
		handle.close();
		
		logger.info("[{}] stop", lsc.uuid());
		lsc.stop();
	}
	
	
	public static void main(String args[]) throws Exception {
		Client01 c = new Client01();
		c.run();
	}
}
