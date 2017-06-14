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
public class PaxNode {
	
	public String name;
	public long treeVersion;
	public long valueVersion;
	public byte[] value;
	
	public PaxNode parent;
	public LinkedList<PaxNode> children;
	
	boolean deleted;
	
	public PaxNode() {
		
	}
	
	public PaxNode(String name) {
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
			for (PaxNode child : children) {
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
	
	public static String rstrip(String s, char c) {
		if (s == null || s.equals(""))
			return s;
		
		int idx = s.length() - 1;
		while (idx >= 0) {
			if (s.charAt(idx) != c)
				break;
			else
				idx--;
		}
		
		if (idx == s.length() - 1)
			return s;
		
		return s.substring(0, idx+1);
	}
	
	public PaxNode find(String path) {
		if (!path.startsWith("/"))
			return null;

		if (path.equals("/") && name.equals(path))
			return this;
		
		path = rstrip(path, '/');
		
		String[] pathSlice = path.split("/");
		if (pathSlice.length < 2)
			return null;
		
		pathSlice[0] = "/";
		
		for (String s : pathSlice) {
			if (s.length() == 0) {
				throw new IllegalArgumentException("invalid path");
			}
		}
		
		return find(pathSlice, 0, pathSlice.length);
	}
	
	/**
	 * 
	 * @param pathSlice example: {"/", "fs", "bigtable", "tablets"} is "/fs/bigtable/tablets"
	 * @param startIdx inclusive
	 * @param endIdx exclusive
	 * @return
	 */
	public PaxNode find(String[] pathSlice, int startIdx, int endIdx) {
		if (startIdx >= endIdx)
			return null;

		if (pathSlice[startIdx].equals(name)) {
			if (startIdx == endIdx - 1)
				return this;
			else {
				if (children == null)
					return null;
				else {
					for (PaxNode node : children) {
						PaxNode ret = node.find(pathSlice, startIdx+1, endIdx);
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
	
	public PaxNode insert(String path, byte[] value) throws Exception {
		if (!path.startsWith("/"))
			throw new IllegalArgumentException("path");
		
		if (path.equals("/") && name.equals(path))
			throw new IllegalArgumentException("path");
		
		path = rstrip(path, '/');
		String[] pathSlice = path.split("/");
		if (pathSlice.length < 2)
			throw new IllegalArgumentException("path");
		
		pathSlice[0] = "/";
		
		for (String s : pathSlice) {
			if (s.length() == 0) {
				throw new IllegalArgumentException("path");
			}
		}
		
		return insert(pathSlice, 1, value);
	}
	
	public PaxNode insert(String[] pathSlice, int startIdx, byte[] value) throws Exception {
		PaxNode child = null;
		if (children != null) {
			for (PaxNode node : children) {
				if (node.name.equals(pathSlice[startIdx])) {
					child = node;
					break;
				}
			}
		}
		
		if (child == null) {
			if (startIdx == pathSlice.length - 1) {
				if (children == null)
					children = new LinkedList<PaxNode>();
				
				PaxNode node = new PaxNode(pathSlice[startIdx]);
				node.value = value;
				node.valueVersion++;
				node.parent = this;
				children.add(node);
				incrVersion();
				
				return node;
			} else {
				throw new PaxNodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
			}
		} else {
			if (startIdx == pathSlice.length - 1)
				throw new PaxNodeException(ErrorMessage.PAX_NODE_EXISTS);
			else {
				return child.insert(pathSlice, startIdx + 1, value);
			}
		}
	}
	
	protected void incrVersion() {
		treeVersion++;
		if (parent != null)
			parent.incrVersion();
	}
	
	public boolean delete(String path) throws Exception {
		PaxNode node = find(path);
		if (node == this)
			throw new PaxNodeException(ErrorMessage.PAX_NODE_PERMISSION);
		
		if (node == null)
			return false;
		
		PaxNode parent = node.parent;
		Iterator<PaxNode> it = parent.children.iterator();
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
	
	public boolean deleteAllChild(String path) throws Exception {
		PaxNode node = find(path);
		if (node == null)
			throw new PaxNodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		if (node.children != null) {
			node.children.stream().forEach((child)->{ child.setDeleted(); });
			node.children.clear();
		}
		incrVersion();
		
		return true;
	}
	
	public boolean updateValue(String path, byte[] value) throws Exception {
		PaxNode node = find(path);
		if (node == null)
			throw new PaxNodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		node.value = value;
		node.valueVersion++;
		
		return true;
	}
	
	public List<PaxNode> listChildren(String path) throws Exception {
		PaxNode node = find(path);
		if (node == null)
			throw new PaxNodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		return node.children;
	}
}
