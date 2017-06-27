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
package com.tchaicatkovsky.lock;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.tchaicatkovsky.pax.common.ErrorMessage;

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
		return find0(pp.cursor());
	}
	
	public Node insert(String path, byte[] data) throws Exception {
		ParsedPath pp = ParsedPath.parse(path);
		Iterator<String> it = pp.iterator();
		it.next();
		return insert(it, data);
	}
	
	public Node find0(ParsedPath.Cursor cursor) {
		if (!cursor.isValid())
			return null;
		
		if (cursor.get().equals(name)) {
			if (cursor.isLast())
				return this;
			else {
				if (children == null || children.size() == 0)
					return null;
				else {
					for (Node child : children) {
						cursor.moveForward();
						Node ret = child.find0(cursor);
						if (ret != null)
							return ret;
						cursor.moveBack();
					}
					return null;
				}
			}
		} else {
			return null;
		}
	}
	
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
				node.treeVersion = 1;
				node.dataVersion++;
				node.parent = this;
				children.add(node);
				incrVersion();
				
				System.out.println(name+" add "+node.name);
				
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
	
	public boolean delete(String path) throws Exception {
		Node node = find(path);
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
	
	public boolean deleteAllChild(String path) throws Exception {
		Node node = find(path);
		if (node == null)
			throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		if (node.children != null) {
			node.children.stream().forEach((child)->{ child.setDeleted(); });
			node.children.clear();
		}
		incrVersion();
		
		return true;
	}
	
	public long updateValue(String path, byte[] data) throws Exception {
		Node node = find(path);
		if (node == null)
			throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		node.data = data;
		node.dataVersion++;
		
		return node.dataVersion;
	}
	
	public List<Node> listChildren(String path) throws Exception {
		Node node = find(path);
		if (node == null)
			throw new NodeException(ErrorMessage.PAX_NODE_NOT_EXISTS);
		
		return node.children;
	}
}
