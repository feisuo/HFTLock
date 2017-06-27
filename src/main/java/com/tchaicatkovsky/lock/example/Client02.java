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
package com.tchaicatkovsky.lock.example;

import com.tchaicatkovsky.lock.Handle;
import com.tchaicatkovsky.lock.HandleFlag;
import com.tchaicatkovsky.lock.LSClient;
import com.tchaicatkovsky.pax.common.Config;

/**
 * @author Teng Huang ht201509@163.com
 */
public class Client02 {
	public void run() throws Exception {
		Config baseConfig = new Config()
				.setUuid(90)
				.setPeers("1:localhost:42221,2:localhost:42222,3:localhost:42223")
				.setName("client");
		

		LSClient lsc = new LSClient(baseConfig);
		lsc.start();
		
		System.out.println("uuid="+lsc.uuid());
		
		Handle handler = lsc.open("/ls/local/bigtable/master", new byte[]{1,2,3}, HandleFlag.EPHEMERAL|HandleFlag.TRY_LOCK);
		
		System.out.println("open success, handler="+handler);
		
		Thread.sleep(1000);
		
		if (handler.isLockHeld()) {
			byte[] data = "234".getBytes();
			
			handler.write(data);
			
			System.out.println("write success, handler="+handler);
			
			Thread.sleep(1000);
			
			byte[] data2 = handler.read();
			
			System.out.println("read success, handler="+handler+", data="+ new String(data2));
		}
		
		Thread.sleep(1000);
		
		handler.close();
		
		Thread.sleep(1000);
		
		lsc.stop();
	}
	
	public static void main(String args[]) throws Exception {
		Client02 c = new Client02();
		c.run();
	}
}
