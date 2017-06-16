package ht.lock;

import java.io.Serializable;
import java.util.LinkedList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ht.pax.util.KryoUtil;

public class NodeInfo implements Serializable, KryoSerializable {
	private static final long serialVersionUID = -763618247875561001L; //NodeInfo
	
	public long treeVersion;
	public long dataVersion;
	public byte[] data;
	public LinkedList<String> childrenName;
	
	public NodeInfo() {
		childrenName = new LinkedList<String>();
	}
	
	public NodeInfo(Node node) {
		childrenName = new LinkedList<String>();
		treeVersion = node.treeVersion;
		dataVersion = node.dataVersion;
		data = node.data;
		if (node.children != null)
			node.children.stream().forEach((c) ->{ childrenName.add(c.name); });
	}
	
	@Override
	public String toString() {
		return String.format("{treeVersion:%d,dataVersion:%d,data:%s,childrenName,%s}",
				treeVersion, dataVersion, data, childrenName);
	}
	
	@Override
	public void write (Kryo kryo, Output output) {
		output.writeLong(treeVersion);
		output.writeLong(dataVersion);
		KryoUtil.writeByteArray(output, data);
		output.writeInt(childrenName.size());
		for (String s : childrenName) 
			output.writeString(s);
	}

	@Override
	public void read (Kryo kryo, Input input) {
		treeVersion = input.readLong();
		dataVersion = input.readLong();
		data = KryoUtil.readByteArray(input);
		int size = input.readInt();
		for (int i = 0; i < size; i++) {
			String s = input.readString();
			childrenName.add(s);
		}
	}
}
