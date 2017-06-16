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
package ht.test.lock;

import static org.junit.Assert.*;
import org.junit.Test;

import ht.lock.Node;

/**
 * @author Teng Huang ht201509@163.com
 */
public class TestNode {
	@Test
	public void testInsert() throws Exception {
		Node root = new Node("/");
		Node node;
		
		assertTrue(root.find("/fs") == null);
		node = root.insert("/fs/", null);
		assertTrue(node != null);
		node = root.find("/fs");
		assertTrue(node != null && node.name.equals("fs"));
		
		assertTrue(root.find("/fs/bigtable") == null);
		node = root.insert("/fs/bigtable", null);
		assertTrue(node != null);
		node = root.find("/fs/bigtable");
		assertTrue(node != null && node.name.equals("bigtable"));
		
		assertTrue(root.find("/fs/bigtable/tablets") == null);
		node = root.insert("/fs/bigtable/tablets", null);
		assertTrue(node != null);
		node = root.find("/fs/bigtable/tablets");
		assertTrue(node != null && node.name.equals("tablets"));
		
		root.print(System.out);
	}
}
