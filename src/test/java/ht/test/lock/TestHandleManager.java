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

import static org.junit.Assert.assertTrue;

import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import com.tchaicatkovsky.lock.HandleContext;
import com.tchaicatkovsky.lock.HandleFD;
import com.tchaicatkovsky.lock.HandleManager;
import com.tchaicatkovsky.pax.internal.exception.PaxInternalException;
import com.tchaicatkovsky.pax.util.ReflectionUtil;

/**
 * @author Teng Huang ht201509@163.com
 */
public class TestHandleManager {
	@Test
	public void testOpen() {
		HandleManager hm = new HandleManager();
		
		String path = "/ls/local/bigtable/master";
		Throwable t = null;
		
		HandleFD fd1 = new HandleFD(1,1);
		HandleContext ctx1 = null;
		try {
			hm.open(fd1, path, null, true, true, null);
		} catch (Exception e) {
			t = e;
		}
		
		ctx1 = hm.getContext(fd1);
		assertNull(t);
		assertTrue(ctx1.isLockHeld());
		
		HandleFD fd2 = new HandleFD(2,1);
		HandleContext ctx2 = null;
		t = null;
		try {
			hm.open(fd2, path, null, true, true, null);
		} catch (Exception e) {
			t = e;
		}
		
		ctx2 = hm.getContext(fd2);
		assertNotNull(t);
		assertTrue(t instanceof PaxInternalException);
		assertTrue(t.getMessage().contains("held"));
		assertTrue(!ctx2.isLockHeld());
		
		hm.close(fd1);
		
		t = null;
		try {
			hm.lock(fd2);
		} catch (Exception e) {
			e.printStackTrace();
			t = e;
		}
		
		assertNull(t);
		assertTrue(ctx2.isLockHeld());
	}
	
	@Test
	public void testOpenInsight01() {
		HandleManager hm = new HandleManager();
		
		String path = "/ls/local/bigtable/master";
		
		Throwable t = null;
		
		HandleFD fd1 = new HandleFD(1,1);
		t = null;
		try {
			hm.open(fd1, path, null, true, false, null); //do not try to lock
		} catch (Exception e) {
			t = e;
		}
		
		HandleContext ctx1 = hm.getContext(fd1);
		assertNull(t);
		assertTrue(!ctx1.isLockHeld());
		
		
		
		HandleFD fd2 = new HandleFD(2,1);
		t = null;
		try {
			hm.open(fd2, path, null, true, true, null); //try to lock
		} catch (Exception e) {
			t = e;
		}
		
		HandleContext ctx2 = hm.getContext(fd2);
		assertNull(t);
		assertTrue(ctx2.isLockHeld());
		
		
		
		HandleFD fd3 = new HandleFD(3,1);
		t = null;
		try {
			hm.open(fd3, path, null, true, true, null); //try to lock
		} catch (Exception e) {
			t = e;
		}
		
		HandleContext ctx3 = hm.getContext(fd3);
		assertNotNull(t);
		assertTrue(!ctx3.isLockHeld());
		assertTrue(t.getMessage().contains("held"));
		
		
		
		Map<HandleFD, HandleContext> watcherList = hm.getWatherList(path);
		assertNotNull(watcherList);
		assertTrue(watcherList.size() == 3);
		assertTrue(watcherList.containsKey(fd1));
		assertTrue(watcherList.containsKey(fd2));
		assertTrue(watcherList.containsKey(fd3));
		
		hm.close(fd2);
		assertTrue(watcherList.size() == 2);
		assertTrue(watcherList.containsKey(fd1));
		assertTrue(watcherList.containsKey(fd3));
		assertTrue(!watcherList.containsKey(fd2));
		assertNull(hm.getContext(fd2));
	}
	
	@Test
	public void testWrite() {
		HandleManager hm = new HandleManager();
		
		String path = "/ls/local/bigtable/master";
		Throwable t = null;
		
		HandleFD fd1 = new HandleFD(1,1);
		
		try {
			hm.open(fd1, path, null, true, true, null);
		} catch (Exception e) {
			
		}
		
		HandleFD fd2 = new HandleFD(2,1);
		try {
			hm.open(fd2, path, null, true, true, null);
		} catch (Exception e) {
			
		}
		
		HandleContext ctx1 = hm.getContext(fd1);
		HandleContext ctx2 = hm.getContext(fd2);
		
		assertTrue(ctx1.node() == ctx2.node());
		
		t = null;
		try {
			hm.write(fd2, new byte[]{1,2,3});
		} catch (Exception e) {
			t = e;
		}
		
		assertNotNull(t);
		assertTrue(t.getMessage().contains("not held"));
		
		t = null;
		byte[] data = new byte[]{2,3,4};
		try {
			hm.write(fd1, data);
		} catch (Exception e) {
			t = e;
		}
		
		assertNull(t);
		assertTrue(ReflectionUtil.isEquals(ctx1.node().data, data));
	}
}


