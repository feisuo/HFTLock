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

import ht.pax.common.HandleEvent;
import ht.pax.common.HandleFD;

/**
 * @author Teng Huang ht201509@163.com
 */
public interface Handle {
	
	HandleFD fd();
	
	String path();
	
	boolean isEphemeral();
	
	public boolean isLockHeld();
	
	byte[] data();
	
	long dataVersion();
	
	HandleEvent pollHandlerEventNotify();
	
	byte[] read() throws Exception;
	
	long write(byte[] data) throws Exception;
	
	boolean lock() throws Exception;
	
	void close() throws Exception;
}
