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
package ht.lock;

import ht.pax.common.Config;
import ht.pax.common.Future;

/**
 * @author Teng Huang ht201509@163.com
 */
public class SubRoutine {
//	private static Logger logger = LoggerFactory.getLogger(SubRoutine.class);
	
	SubRoutineAsync asb;
	long uuid;
	
	public SubRoutine(Config config) {
		asb = new SubRoutineAsync(config);
	}
	
	public long uuid() {
		return uuid;
	}
	
	public SubRoutineAsync asb() {
		return asb;
	}
	
	public void start() throws Exception {
		asb.start();
		uuid = applyForUuid();
	}
	
	public void stop() {
		asb.stop();
	}
	
	protected long applyForUuid() throws Exception {
		Future<Long> f = asb.applyForUuid();
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		uuid = f.get();
		return uuid;
	}
	
	public long put(String key, byte[] value, long expectedKeyVersion)  throws Exception {
		Future<Long> f = asb.put(key, value, expectedKeyVersion);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
	
	public Object get(String key) throws Exception {	
		Future<Object> f = asb.get(key);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
	
	public boolean createNode(String path, byte[] value) throws Exception {
		Future<Void> f = asb.createNode(path, value);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return true;
	}
	
	public Handle open(String path, byte[] data, int flag) throws Exception {
		Future<Handle> f = asb.open(path, data, flag);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
}
