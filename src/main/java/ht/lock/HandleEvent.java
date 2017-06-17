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

import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author Teng Huang ht201509@163.com
 */
public class HandleEvent implements Serializable, KryoSerializable {
	
	private static final long serialVersionUID = -763618247875551016L; //HandleEvent
	
	final static int EVENT_TYPE_LOCK = 1;
	final static int EVENT_TYPE_UNLOCK = 1 << 1;
	final static int EVENT_TYPE_NODE_CHILDREN_CHANGE = 1 << 2;
	
	public HandleFD fd;
	public int type;
	
	public HandleEvent() {
		
	}
	
	public HandleEvent(HandleFD fd) {
		this.fd = fd;
	}
	
	public boolean isLock() {
		return (type & EVENT_TYPE_LOCK) != 0;
	}
	
	public void setLock() {
		type |= EVENT_TYPE_LOCK;
	}
	
	public boolean isUnlock() {
		return  (type & EVENT_TYPE_UNLOCK) != 0;
	}
	
	public void setUnlock() {
		type |= EVENT_TYPE_UNLOCK;
	}
	
	public boolean isNodeChildrenChange() {
		return (type & EVENT_TYPE_NODE_CHILDREN_CHANGE) != 0;
	}
	
	public void setNodeChildrenChange() {
		type |= EVENT_TYPE_NODE_CHILDREN_CHANGE;
	}
	
	@Override
	public String toString() {
		return String.format("{fd:%s,type:%s}", fd, Integer.toBinaryString(type));
	}
	
	@Override
	public void write (Kryo kryo, Output output) {
		kryo.writeClassAndObject(output, fd);
		output.writeInt(type);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		fd = (HandleFD)kryo.readClassAndObject(input);
		type = input.readInt();
	}
}
