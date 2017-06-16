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

import ht.pax.util.KryoUtil;

/**
 * @author Teng Huang ht201509@163.com
 */
public class PaxOperationNodeCreate extends PaxOperationNode {
	private static final long serialVersionUID = -763618247875563008L; //PaxOperationNodeCreate
	
	public byte[] data;
	
	public PaxOperationNodeCreate() {
		
	}
	
	public PaxOperationNodeCreate(long uuid, long luid, String path, byte[] data, long version, long valueVersion) {
		super(uuid, luid, PaxOperationNode.CREATE, path);
		this.data = data;
	}
	
	@Override
	public String toString() {
		return String.format("{uuid:%d,luid:%d,opType:createNode,path:%s,data:%s}", 
				uuid, luid, path, data);
	}
	
	@Override
	public void write (Kryo kryo, Output output) {
		super.write(kryo, output);
		KryoUtil.writeByteArray(output, data);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		super.read(kryo, input);
		data = KryoUtil.readByteArray(input);
	}
}
