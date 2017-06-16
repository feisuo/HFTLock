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

import ht.pax.common.HandleFD;
import ht.pax.common.PaxOperation;
import ht.pax.util.KryoUtil;

/**
 * @author Teng Huang ht201509@163.com
 */
public class PaxOperationHandleOpen extends PaxOperation {
	private static final long serialVersionUID = -763618247875563010L; //PaxOperationHandlerOpen
	
	public HandleFD fd;
	public String path;
	public byte[] data;
	public int flag;
	
	public PaxOperationHandleOpen() {
		
	}
	
	public PaxOperationHandleOpen(long uuid, long luid, HandleFD fd, String path, byte[] data, int flag) {
		super(uuid, luid);
		this.fd = fd;
		this.path = path;
		this.data = data;
		this.flag = flag;
	}
	
	@Override
	public void write (Kryo kryo, Output output) {
		super.write(kryo, output);
		kryo.writeClassAndObject(output, fd);
		output.writeString(path);
		output.writeInt(flag);
		KryoUtil.writeByteArray(output, data);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		super.read(kryo, input);
		fd = (HandleFD)kryo.readClassAndObject(input);
		path = input.readString();
		flag = input.readInt();
		data = KryoUtil.readByteArray(input);
	}
	
	@Override
	public String toString() {
		return String.format("{uuid:%d,luid:%d,type:openHandler,fd:%s,path:%s,data:%s,flag:%d}", 
				uuid, luid, fd, path, data, flag);
	}
}

