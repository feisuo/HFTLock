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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import ht.pax.cell.Cell;
import ht.pax.common.Config;
import ht.lock.CellLockService;

/**
 * @author Teng Huang ht201509@163.com
 */
public class LockServiceCell {
	public static void main(String args[]) throws Exception {
		
		InputStream in = new FileInputStream(args[0]);
		Properties pro = new Properties();
		pro.load(in);
		in.close();
		
		long uuid = Long.valueOf(pro.getProperty("uuid"));
		String peers = pro.getProperty("peers"); //1:localhost:42221,2:localhost:42222,3:localhost:42223
		String dataPath = pro.getProperty("dataPath");
		
		Config config = new Config().setUuid(uuid).setPeers(peers).setDataDir(dataPath);
		
		Cell p = Cell.create(config, new CellLockService());
		
		p.start();
		
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				break;
			}
		}
		
		p.stop();
	}
}
