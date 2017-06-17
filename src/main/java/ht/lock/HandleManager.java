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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import ht.pax.internal.exception.PaxInternalException;

/**
 * @author Teng Huang ht201509@163.com
 */
public class HandleManager {
	
	HashMap<HandleFD, HandleContext> handleMap = new HashMap<>();
	HashMap<String, HashMap<HandleFD, HandleContext>> path2WatcherList = new HashMap<>();
	HashMap<String, HandleContext> path2LockHolder = new HashMap<>();
	
	public HandleContext getLockHelderContext(String path) {
		return path2LockHolder.get(path);
	}
	
	public HandleContext getContext(HandleFD fd) {
		return handleMap.get(fd);
	}
	
	public Map<HandleFD, HandleContext> getWatherList(String path) {
		return path2WatcherList.get(path);
	}
	
	protected HandleContext firstWatcher(Map<HandleFD, HandleContext> watcherList) {
		if (watcherList == null || watcherList.size() == 0)
			return null;
		
		Iterator<HandleContext> it = watcherList.values().iterator();
		return it.next();
	}
	
	public void open(HandleFD fd, String path, byte[] data, 
			boolean ephemeral,
			boolean tryLock,
			Node persistentNode) throws Exception {
		
		HandleContext h = handleMap.get(fd);
		if (h != null)
			throw new PaxInternalException("reopen");
		
		HandleContext newCtx = new HandleContext(fd, path);
		
		HashMap<HandleFD, HandleContext> watcherList = path2WatcherList.get(path);
		if (watcherList == null) {
			watcherList = new HashMap<HandleFD, HandleContext>();
			path2WatcherList.put(path, watcherList);
		}
		
		if (ephemeral) {
			newCtx.node = (watcherList.size() == 0 ? new Node() : firstWatcher(watcherList).node);
		} else {
			newCtx.node = persistentNode;
		}
		
		handleMap.put(fd, newCtx);
		watcherList.put(fd, newCtx);
		
		if (tryLock) {
			try {
				boolean ret = lock(newCtx.fd);
				if (ret) {
					write(fd, data);
				}
			} catch (Exception e) {
				throw e;
			}
		}
	}
	
	public boolean lock(HandleFD fd) throws Exception {
		HandleContext ctx = handleMap.get(fd);
		if (ctx == null) {
			throw new PaxInternalException("null ctx");
		}
		
		HandleContext lockHeldCtx = path2LockHolder.get(ctx.path);
		if (lockHeldCtx != null)
			throw new PaxInternalException("held");
		
		path2LockHolder.put(ctx.path, ctx);
		ctx.setLockHeld(true);
		
		return true;
	}
	
	public byte[] read(HandleFD fd) throws Exception {
		
		HandleContext ctx = handleMap.get(fd);
		if (ctx == null) {
			throw new PaxInternalException("null ctx");
		}
		
		return ctx.node.data;
	}
	
	/*
	 * @return: version of node value.
	 */
	public long write(HandleFD fd, byte[] data) throws Exception {
		
		HandleContext ctx = handleMap.get(fd);
		if (ctx == null) {
			throw new PaxInternalException("null ctx");
		}
		
		if (!ctx.isLockHeld())
			throw new PaxInternalException("not held");
		
		ctx.node.data = data;
		ctx.node.dataVersion++;
		
		return ctx.node.dataVersion;
	}
	
	public HandleContext close(HandleFD fd) {		
		HandleContext ctx = handleMap.remove(fd);

		if (ctx != null) {
			if (ctx.isLockHeld())
				path2LockHolder.remove(ctx.path);
			removeFromWatcherList(ctx);
		}
		
		return ctx;
	}
	
	public List<HandleContext> close(long uuid) {
		List<HandleContext> ret = new LinkedList<HandleContext>();
		
		Iterator<Map.Entry<HandleFD, HandleContext>> it = handleMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<HandleFD, HandleContext> e = it.next();
			HandleFD fd = e.getKey();
			HandleContext ctx = e.getValue();
			
			if (fd.uuid == uuid) {
				ret.add(ctx);
				it.remove();
				
				if (ctx.isLockHeld())
					path2LockHolder.remove(ctx.path);
				removeFromWatcherList(ctx);
			}
		}
		
		return ret;
	}
	
	protected void removeFromWatcherList(HandleContext ctx) {
		Map<HandleFD, HandleContext> ctxList = path2WatcherList.get(ctx.path);
		if (ctxList != null && ctxList.size() > 0)
			ctxList.remove(ctx.fd());
		
		if (ctxList.size() == 0)
			path2WatcherList.remove(ctx.path);
	}
}

