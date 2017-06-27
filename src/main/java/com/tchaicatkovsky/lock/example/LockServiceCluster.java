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

import com.tchaicatkovsky.pax.cell.Cell;
import com.tchaicatkovsky.lock.CellLockService;
import com.tchaicatkovsky.pax.common.Config;
import com.tchaicatkovsky.pax.common.LeaderPolicy;
import com.tchaicatkovsky.pax.util.FileUtil;

/**
 * @author Teng Huang ht201509@163.com
 */
public class LockServiceCluster {
	public static void main(String args[]) throws Exception {
		
		FileUtil.deletePath("./data/BasicPaxos");
		
		Config baseConfig = new Config()
				.setPeers("1:localhost:42221,2:localhost:42222,3:localhost:42223")
				.setLeaderPolicy(LeaderPolicy.DEFAULT);
		
		Config ca = baseConfig.clone().setUuid(1).setDataDir("./data/BasicPaxos/node1");
		Config cb = baseConfig.clone().setUuid(2).setDataDir("./data/BasicPaxos/node2");
		Config cc = baseConfig.clone().setUuid(3).setDataDir("./data/BasicPaxos/node3");
		
		Cell pr1 = Cell.create(ca, new CellLockService());
		Cell pr2 = Cell.create(cb, new CellLockService());
		Cell pr3 = Cell.create(cc, new CellLockService());
		
		pr1.start();
		pr2.start();
		pr3.start();
		
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				break;
			}
		}
		
		pr1.stop();
		pr2.stop();
		pr3.stop();
	}
}
