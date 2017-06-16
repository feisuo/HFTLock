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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import ht.pax.common.ErrorMessage;

/**
 * @author Teng Huang ht201509@163.com
 */
public class Node {
	
	public String name;
	public long treeVersion;
	public long dataVersion;
	public byte[] data;
	
	public Node parent;
	public LinkedList<Node> children;
	
	boolean deleted;
	
	public Node() {
		
	}
	
	public Node(String name) {
		this.name = name;
	}
	
	public void setDeleted() {
		deleted = true;
		if (children != null)
			children.stream().forEach((child)->{ child.setDeleted(); });
	}
	
	public boolean isDeleted() {
		return deleted;
	}
	
	public void print(PrintStream ps) {
		print("", ps);
	}
	
	public void print(String indent, PrintStream ps) {
		ps.println(indent+name);
		if (children != null) {
			for (Node child : children) {
				child.print(indent+"\t", ps);
			}
		}
	}
	
	@Override
	public String toString() {
		List<String> nodeName = new ArrayList<String>();
		if (children != null) {
			children.stream().forEach((n)->{nodeName.add(n.name);});
		}
		
		return String.format("{name:%s,children:%s}", name, nodeName);
	}
	
	
	public Node find(String path) throws Exception {
		ParsedPath pp = ParsedPath.parse(path);
		return find(pp.iterator());
	}
	
	public Node insert(String path, byte[] data) throws Exception {
		ParsedPath pp = ParsedPath.parse(path);
		Iterator<String> it = pp.iterator();
		it.next();
		return insert(it, data);
	}
	
	public boolean delete(String path) throws Exception {
		ParsedPath pp = ParsedPath.parse(path);
		return delete(pp.iterator());
	}
	
	public boolean deleteAllChild(String path) throws Exception {
		ParsedPath pp = ParsedPath.parse(path);
		return deleteAllChild(pp.iterator());
	}
	
	public long updateValue(String path, byte[] data) throws Exception {
		ParsedPath pp = ParsedPath.parse(path);
		return updateValue(pp.iterator(), data);		
	}
	
	public List<Node> listChildren(String path) throws Exception {
		ParsedPath pp = ParsedPath.parse(path);
		return listChildren(pp.iterator());	
	}

	
	/**
	 * 
	 * @param pathSlice example: {"/", "fs", "bigtable", "tablets"} is "/fs/bigtable/tablets"
	 * @param startIdx inclusive
	 * @param endIdx exclusive
	 * @return
	 */
//	public Node find(String[] pathSlice, int startIdx, int endIdx) {
//		if (startIdx >= endIdx)
//			return null;
//
//		if (pathSlice[startIdx].equals(name)) {
//			if (startIdx == endIdx - 1)
//				return this;
//			else {
//				if (children == null)
//					return null;
//				else {
//					for (Node node : children) {
//						Node ret = node.find(pathSlice, startIdx+1, endIdx);
//						if (ret != null)
//							return ret;
//					}
//					return null;
//				}
//			}
//		} else {
//			return null;
//		}
//	}
	
	public Node find(Iterator<String> pathIt) {
		if (!pathIt.hasNext())
			return null;
		
		
		boolean hasNext1 = pathIt.hasNext();
		String slice = pathIt.next();
		boolean hasNext2 = pathIt.hasNext();
		boolean isLast = (hasNext1 && !hasNext2);
		
		if (slice.equals(name)) {
			if (isLast)
				return this;
			else {
				if (children == null)
					return null;
				else {
					for (Node node : children) {
						Node ret = node.find(pathIt);
						if (ret != null)
							return ret;
					}
					return null;
				}
			}
		} else {
			return null;
		}
	}
//	
//	public Node insert(String[] pathSlice, int startIdx, byte[] value) throws Exception {
//		Node child = null;
//		if (children != null) {
//			for (Node node : children) {
//				if (node.name.equals(pathSlice[startIdx])) {
//					child = node;
//					break;
//				}
//			}
//		}
//		
//		if (child == null) {
//			if (startIdx == pathSlice.length - 1) {
//				if (children == null)
//					children = new LinkedList<Node>();
//				
//				Node node = new Node(pathSlice[startIdx]);
//				node.data = value;
//				node.dataVersion++;
//				node.parent = this;
//				children.add(node);
//				incrVersion();
//				
//				return node;
//			} else {
//				throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
//			}
//		} else {
//			if (startIdx == pathSlice.length - 1)
//				throw new NodeException(ErrorMessage.PAX_NODE_EXISTS);
//			else {
//				return child.insert(pathSlice, startIdx + 1, value);
//			}
//		}
//	}
	
	Node insert(Iterator<String> pathIt, byte[] value) throws Exception {
		assert(pathIt.hasNext());
		boolean hasNext1 = pathIt.hasNext();
		String slice = pathIt.next();
		boolean hasNext2 = pathIt.hasNext();
		boolean isLast = (hasNext1 && !hasNext2);
		
		Node child = null;
		if (children != null) {
			for (Node node : children) {
				if (node.name.equals(slice)) {
					child = node;
					break;
				}
			}
		}
		
		if (child == null) {
			if (isLast) {
				if (children == null)
					children = new LinkedList<Node>();
				
				Node node = new Node(slice);
				node.data = value;
				node.dataVersion++;
				node.parent = this;
				children.add(node);
				incrVersion();
				
				return node;
			} else {
				throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
			}
		} else {
			if (isLast)
				throw new NodeException(ErrorMessage.PAX_NODE_EXISTS);
			else {
				return child.insert(pathIt, value);
			}
		}
	}
	
	protected void incrVersion() {
		treeVersion++;
		if (parent != null)
			parent.incrVersion();
	}
	
	public boolean delete(Iterator<String> pathIt) throws Exception {
		Node node = find(pathIt);
		if (node == this)
			throw new NodeException(ErrorMessage.PAX_NODE_PERMISSION);
		
		if (node == null)
			return false;
		
		Node parent = node.parent;
		Iterator<Node> it = parent.children.iterator();
		while (it.hasNext()) {
			if (it.next() == node) {
				it.remove();
				incrVersion();
				return true;
			}
		}
		
		node.setDeleted();
		
		return true;
	}
	
	public boolean deleteAllChild(Iterator<String> pathIt) throws Exception {
		Node node = find(pathIt);
		if (node == null)
			throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		if (node.children != null) {
			node.children.stream().forEach((child)->{ child.setDeleted(); });
			node.children.clear();
		}
		incrVersion();
		
		return true;
	}
	
	public long updateValue(Iterator<String> pathIt, byte[] data) throws Exception {
		Node node = find(pathIt);
		if (node == null)
			throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		node.data = data;
		node.dataVersion++;
		
		return node.dataVersion;
	}
	
	public List<Node> listChildren(Iterator<String> pathIt) throws Exception {
		Node node = find(pathIt);
		if (node == null)
			throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		return node.children;
	}
}