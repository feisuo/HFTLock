package com.tchaicatkovsky.lock;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tchaicatkovsky.pax.common.PaxOperationResult;

public class PaxOperationResultNodeQuery extends PaxOperationResult {
	private static final long serialVersionUID = -763618247875564007L; //PaxOperationResultNodeUpdate
	
	public NodeInfo nodeInfo;
	
	public PaxOperationResultNodeQuery() {
		
	}
	
	public PaxOperationResultNodeQuery(long replicaVersion, boolean success, String errorMsg, NodeInfo nodeInfo) {
		super(replicaVersion, success, errorMsg);
		this.nodeInfo = nodeInfo;
	}
	
	@Override
	public String toString() {
		return String.format("{replicaVersion:%d,success:%s,errorMsg:%s,nodeInfo:%s}",
				replicaVersion, success, errorMsg, nodeInfo);
	}
	
	@Override
	public void write (Kryo kryo, Output output) {
		super.write(kryo, output);
		kryo.writeClassAndObject(output, nodeInfo);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		super.read(kryo, input);
		nodeInfo = (NodeInfo)kryo.readClassAndObject(input);
	}
}
