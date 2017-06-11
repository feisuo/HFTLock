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

import ht.pax.internal.common.op.PaxOperationResultUpdate;
import ht.pax.util.KryoUtil;

/**
 * @author Teng Huang ht201509@163.com
 */
public class PaxOperationResultHandler extends PaxOperationResultUpdate {
	private static final long serialVersionUID = -763618247875554005L; //PaxOperationResultHandler
	
	public boolean lockHeld;
	public long dataVersion;
	public byte[] data;
	
	public PaxOperationResultHandler() {
		
	}
	
	public PaxOperationResultHandler(boolean lockHeld, byte[] data, long dataVersion) {
		this.lockHeld = lockHeld;
		this.data = data;
		this.dataVersion = dataVersion;
	}
	
	@Override
	public String toString() {
		return String.format("{iid:%d, errorMsg:%s, committedBefore:%s,lockHeld:%s,data:%s,dataVersion:%d}",
				replicaVersion, errorMsg, committedBefore, lockHeld, data, dataVersion);
	}
	
	@Override
	public void write (Kryo kryo, Output output) {
		super.write(kryo, output);
		output.writeBoolean(lockHeld);
		output.writeLong(dataVersion);
		KryoUtil.writeByteArray(output, data);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		super.read(kryo, input);
		lockHeld = input.readBoolean();
		dataVersion = input.readLong();
		data = KryoUtil.readByteArray(input);
	}
}
