package ht.lock;

import ht.lock.PaxNode;

public class NodeTree {
	/**
	 * pnode subsystem 
	 */
	PaxNode pnodeRoot = new PaxNode("/");
	
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
	
	public boolean update(String path, byte[] value) throws Exception {
		pnodeRoot.updateValue(path, value);
		return false;
	}
}
