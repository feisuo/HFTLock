package ht.lock;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ht.pax.common.PaxOperationResult;

public class PaxOperationResultNodeUpdate extends PaxOperationResult {
	private static final long serialVersionUID = -763618247875564006L; //PaxOperationResultNodeUpdate
	
	public long dataVersion;
	
	public PaxOperationResultNodeUpdate() {
		
	}
	
	public PaxOperationResultNodeUpdate(long replicaVersion, boolean success, String errorMsg, long dataVersion) {
		super(replicaVersion, success, errorMsg);
		this.dataVersion = dataVersion;
	}
	
	@Override
	public String toString() {
		return String.format("{replicaVersion:%d,success:%s,errorMsg:%s,dataVersion:%d}",
				replicaVersion, success, errorMsg, dataVersion);
	}
	
	@Override
	public void write (Kryo kryo, Output output) {
		super.write(kryo, output);
		output.writeLong(dataVersion);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		super.read(kryo, input);
		dataVersion = input.readLong();
	}
}
