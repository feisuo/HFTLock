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

import ht.pax.common.Config;
import ht.pax.common.HandleEvent;
import ht.pax.common.HandlerFlag;
import ht.lock.Handle;
import ht.lock.SubRoutine;

/**
 * @author Teng Huang ht201509@163.com
 */
public class Client03 {
	
	private static Logger logger = LoggerFactory.getLogger(Client03.class);
	
	public void run() throws Exception {
		Config baseConfig = new Config()
				.setUuid(90)
				.setPeers("1:localhost:42221,2:localhost:42222,3:localhost:42223")
				.setName("client");
		

		SubRoutine sb = new SubRoutine(baseConfig);
		sb.start();
		
		Handle handle = sb.open("/ls/local/bigtable/master", new byte[]{1,2,3}, HandlerFlag.EPHEMERAL|HandlerFlag.TRY_LOCK);
		
		logger.info("[{}] open success, handler={}", sb.uuid(), handle);
		
		while (true) {
			HandleEvent event = handle.pollHandlerEventNotify();
			if (event != null) {
				if (event.isUnlock() && !handle.isLockHeld()) {
					logger.info("[{}] unlock event, try to lock, handle={}", sb.uuid(), handle);
					
					try {
						handle.lock();
						logger.info("[{}] lock success, handle={}", sb.uuid(), handle);
					} catch (Exception e) {
						logger.warn("[{}] lock failed, handle={}, errorMsg={}", sb.uuid(), handle, e.getMessage());
					}
				}
			}
			
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				break;
			}
		}
		
		//handle.close();
		
		Thread.sleep(1000);
		
		sb.stop();
	}
	
	public static void main(String args[]) throws Exception {
		Client03 c = new Client03();
		c.run();
	}
}
