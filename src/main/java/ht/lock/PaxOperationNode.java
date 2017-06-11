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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ht.pax.common.PaxOperation;

/**
 * @author Teng Huang ht201509@163.com
 */
public class PaxOperationNode extends PaxOperation {

	private static final long serialVersionUID = -763618247875553007L; //PaxOperationNode
	
	public final static byte CREATE = 1;
	public final static byte DELETE = 2;
	public final static byte UPDATE_VALUE = 3;
	public final static byte DELETE_ALL_CHILD = 4;
	public final static byte LIST_CHILDREN = 5;
	
	public byte opType;
	public String path;
	
	public PaxOperationNode() {
		
	}
	
	public PaxOperationNode(long uuid, long luid, byte opType, String path) {
		super(uuid, luid);
		this.opType = opType;
		this.path = path;
	}
	

	@Override
	public String toString() {
		return String.format("{uuid:%d,luid:%d,opType:%d,path:%s}", uuid, luid, opType, path);
	}
	

	@Override
	public void write (Kryo kryo, Output output) {
		super.write(kryo, output);
		output.writeByte(opType);
		output.writeString(path);
	}
	

	@Override
	public void read (Kryo kryo, Input input) {
		super.read(kryo, input);
		opType = input.readByte();
		path = input.readString();
	}
}
