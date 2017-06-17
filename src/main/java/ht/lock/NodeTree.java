package ht.lock;

import ht.lock.Node;

public class NodeTree {
	/**
	 * pnode subsystem 
	 */
	Node pnodeRoot = new Node("/");
	
	public Node root() {
		return pnodeRoot;
	}
	
	public Node find(String path) throws Exception {
		return pnodeRoot.find(path);
	}
	
	public boolean insert(String path, byte[] value) throws Exception {
		pnodeRoot.insert(path, value);
		return false;
	}
	
	public boolean delete(String path) throws Exception {
		pnodeRoot.delete(path);
		return false;
	}
	
	public boolean deleteAllChildren(String path) throws Exception {
		pnodeRoot.deleteAllChild(path);
		return false;
	}
	
	/**
	 * 
	 * @param path
	 * @param value
	 * @return the dataVersion;
	 * @throws Exception
	 */
	public long update(String path, byte[] value) throws Exception {
		return pnodeRoot.updateValue(path, value);
	}
}
