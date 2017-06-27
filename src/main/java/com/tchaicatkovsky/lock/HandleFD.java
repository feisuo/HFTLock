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
package com.tchaicatkovsky.lock;

import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author Teng Huang ht201509@163.com
 */
final public class HandleFD implements Serializable, KryoSerializable {

	private static final long serialVersionUID = -763618247875551015L; //HandleFD
	
	public long uuid;
	public long luid;
	
	public HandleFD(long uuid, long luid) {
		this.uuid = uuid;
		this.luid = luid;
	}
	
	@Override
	public int hashCode() {
		int h = 0;
		h = h * 31 + Long.hashCode(uuid);
		h = h * 31 + Long.hashCode(luid);
		return h;
	}
	
	@Override
	public boolean equals(Object o) {
		HandleFD hfd = (HandleFD)o;
		return uuid == hfd.uuid && luid == hfd.luid;
	}
	
	@Override
	public String toString() {
		return String.format("{uuid:%d,luid:%d}", uuid, luid);
	}
	

	@Override
	public void write (Kryo kryo, Output output) {
		output.writeLong(uuid);
		output.writeLong(luid);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		uuid = input.readLong();
		luid = input.readLong();
	}
}

