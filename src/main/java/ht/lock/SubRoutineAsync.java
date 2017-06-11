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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ht.pax.internal.event.Messager;
import ht.pax.internal.message.ClientResp;
import ht.pax.internal.message.EventNotify;
import ht.pax.internal.message.MessageId;
import ht.pax.kvservice.PaxOperationKV;
import io.netty.util.concurrent.ScheduledFuture;
import ht.pax.internal.message.ClientApplyForUuidReq;
import ht.pax.internal.message.ClientApplyForUuidResp;
import ht.pax.internal.message.ClientGetLeaderReq;
import ht.pax.internal.message.ClientGetLeaderResp;
import ht.pax.internal.message.ClientReq;
import ht.pax.common.ClientConnection;
import ht.pax.common.Command;
import ht.pax.common.Config;
import ht.pax.common.DefaultFuture;
import ht.pax.common.ErrorMessage;
import ht.pax.common.Future;
import ht.pax.common.HandleEvent;
import ht.pax.common.HandleFD;
import ht.pax.common.HandlerFlag;
import ht.pax.common.LeaderInfo;
import ht.pax.common.Message;
import ht.pax.internal.common.op.PaxOperationResultErrorLeader;
import ht.pax.internal.common.op.PaxOperationResultQuery;
import ht.pax.internal.common.op.PaxOperationResultUpdate;
import ht.pax.internal.event.ClientEventEngine;
import ht.pax.internal.event.EventEngineListener;

/**
 * @author Teng Huang ht201509@163.com
 */
public class SubRoutineAsync implements EventEngineListener {
	private static Logger logger = LoggerFactory.getLogger(SubRoutineAsync.class);
	
	long uuid;
	boolean started;
	ScheduledFuture<?> keepLivenessTicker;
	Messager messager;
	ClientEventEngine clientEventEngine;
	long requestTimeoutMillis;
	long maxReplicaVersion = -1;
	HashSet<Long> maxReplicaVersionReplicaSet = new HashSet<>();
	GetLeaderContext getLeaderCtx = new GetLeaderContext();
	LinkedList<RequestContext> reqCtxQueue = new LinkedList<RequestContext>();

	HashMap<HandleFD, HandleImpl> handleMap = new HashMap<HandleFD, HandleImpl>();
	
	AtomicLong isn = new AtomicLong(1);
	protected long genIsn() {
		return isn.incrementAndGet();
	}
	
	public class RequestContext {
		Message req;
		long luid;
		Future<?> future;
		boolean running;
		int tryCnt;
		long createTime;
		
		public RequestContext(Message req, long luid, Future<?> future) {
			this.req = req;
			this.luid = luid;
			this.future = future;
			running = false;
			tryCnt = 0;
			createTime = System.currentTimeMillis();
		}
		
		@Override
		public String toString() {
			return String.format("{req:%s,luid:%d,running:%s,tryCnt:%d}", req, luid, running, tryCnt);
		}
	}
	
	public class GetLeaderContext extends RequestContext {
		LeaderInfo leaderInfo;
		boolean established;
		long startTime;
		HashMap<Long, ClientGetLeaderResp> respMap = new HashMap<Long, ClientGetLeaderResp>();
		
		public GetLeaderContext() {
			super(null, 0, null);
			leaderInfo = new LeaderInfo();
		}
		
		public String toString() {
			return String.format("{running:%s,established:%s,leaderInfo:%s}", running, established, leaderInfo);
		}
		
		public boolean isEstablished() {
			return established;
		}
		
		public LeaderInfo getLeaderInfo() {
			return leaderInfo;
		}
		
		public boolean isRunning() {
			return running;
		}
		
		public void onResp(ClientGetLeaderResp resp) {
			if (logger.isDebugEnabled()) logger.debug("[{}] onClientGetLeaderResp, resp={}, this={}",
					uuid(), resp, this);
			
			if (!running)
				return;
			
			if (luid != resp.luid)
				return;
			
			ClientGetLeaderResp oldResp = respMap.get(resp.srcUuid);
			if (oldResp == null || oldResp.leaderInfo.epoch < resp.leaderInfo.epoch)
				respMap.put(resp.srcUuid, resp);
			
			if (respMap.size() < quorum())
				return;
			
			respMap.entrySet().stream().forEach((e)->{
				if (e.getValue().leaderInfo.epoch > leaderInfo.epoch)
					leaderInfo = e.getValue().leaderInfo;
			});
			
			established = true;
			stop();
			
			if (logger.isDebugEnabled()) logger.debug("[{}] new leader established, leaderInfo={}",
					uuid(), leaderInfo);
		}
		
		public void start() {
			if (running)
				return;
			
			
			luid = genIsn();
			req = new ClientGetLeaderReq(uuid(), luid);
			running = true;
			established = false;
			respMap.clear();
			startTime = System.currentTimeMillis();
			
			messager.broadcast(req);
			
			if (logger.isDebugEnabled()) logger.debug("[{}] GetLeaderContext::start, req={}", uuid(), req);
		}
		
		public void stop() {
			running = false;
		}
		
		public void checkTime(long curTime) {
			if (!running)
				return;
			
			if (curTime - startTime > requestTimeoutMillis) {
				stop();
				start();
			}
		}
	}
	
	public class HandleImpl implements Handle {
		
		final HandleFD fd;
		final String path;
		boolean ephemeral;
		boolean lockHeld;
		byte[] data;
		long dataVersion;
		ConcurrentLinkedQueue<HandleEvent> handleEventQueue = new ConcurrentLinkedQueue<>();
		
		public HandleImpl(HandleFD fd, String path, boolean ephemeral) {
			this.fd = fd;
			this.path = path;
			this.ephemeral = ephemeral;
		}
		
		@Override
		public HandleFD fd() {
			return fd;
		}
		
		@Override
		public String path() {
			return path;
		}
		
		@Override
		public boolean isEphemeral() {
			synchronized(SubRoutineAsync.this) {
				return ephemeral;
			}
		}
		
		public void setEphemeral(boolean b) {
			synchronized(SubRoutineAsync.this) {
				ephemeral = b;
			}
		}
		
		@Override
		public boolean isLockHeld() {
			synchronized(SubRoutineAsync.this) {
				return lockHeld;
			}
		}
		
		public void setLockHeld(boolean b) {
			synchronized(SubRoutineAsync.this) {
				lockHeld = b;
			}
		}
		
		@Override
		public byte[] data() {
			synchronized(SubRoutineAsync.this) {
				return data;
			}
		}
		
		public void setData(byte[] data) {
			synchronized(SubRoutineAsync.this) {
				this.data = data;
			}
		}
		
		@Override
		public long dataVersion() {
			synchronized(SubRoutineAsync.this) {
				return dataVersion;
			}
		}
		
		public void setDataVersoin(long dataVersion) {
			synchronized(SubRoutineAsync.this) {
				this.dataVersion = dataVersion;
			}
		}
		
		public void offerHandlerEventNotify(HandleEvent event) {
			handleEventQueue.offer(event);
		}
		
		@Override
		public HandleEvent pollHandlerEventNotify() {
			return handleEventQueue.poll();
		}
		
		@Override
		public byte[] read() throws Exception {
			Future<byte[]> f = SubRoutineAsync.this.read(this);
			f.sync();
			if (!f.isSuccess())
				throw new PaxClientException(f.errorMsg());
			return f.get();
		}
		
		@Override
		public long write(byte[] data) throws Exception {
			Future<Long> f = SubRoutineAsync.this.write(this, data);
			f.sync();
			if (!f.isSuccess())
				throw new PaxClientException(f.errorMsg());
			return f.get();
		}
		
		@Override
		public boolean lock() throws Exception {
			Future<Boolean> f = SubRoutineAsync.this.lock(this);
			f.sync();
			if (!f.isSuccess())
				throw new PaxClientException(f.errorMsg());
			return f.get();
		}
		
		@Override
		public void close() throws Exception {
			Future<Void> f = SubRoutineAsync.this.close(this);
			f.sync();
			if (!f.isSuccess())
				throw new PaxClientException(f.errorMsg());
		}
		
		@Override
		public synchronized String toString() {
			return String.format("{fd:%s,path:%s,dataVersion:%d,lockHeld:%s}", 
					fd, path, dataVersion, lockHeld);		
		}
	}
	
	public SubRoutineAsync(Config config) {
		uuid = 0;
		started = false;
		clientEventEngine = new ClientEventEngine(config);	
		messager = clientEventEngine.getMessager();
		clientEventEngine.addEventEngineListener(this);
		requestTimeoutMillis =  config.getDefaultClientRequestTimeoutMillis();
	}
	
	Runnable keepLivenessTickerRunnable = new Runnable() {
		@Override
		public synchronized void run() {
			if (uuid == 0)
				return;

			RequestContext reqCtx = reqCtxQueue.peekFirst();
			while (reqCtx != null) {
				long curTime = System.currentTimeMillis();
				if (curTime - reqCtx.createTime > requestTimeoutMillis) {
					reqCtxQueue.removeFirst();
					reqCtx.future.finish(false, ErrorMessage.PAX_TIMEOUT, null);
					reqCtx = reqCtxQueue.peekFirst();
					continue;
				}
				
				getLeaderCtx.checkTime(curTime);
				
				executeNextRequest();
				
				break;
			}
		}
	};
	
	public synchronized void start() {
		if (started) 
			return;
					
		clientEventEngine.start();
		clientEventEngine.connectAllPeer();
		
		started = true;
		
		keepLivenessTicker = clientEventEngine.scheduleAtFixedRateMillis(keepLivenessTickerRunnable, 0, 1000);
	}
	
	public synchronized void stop() {
		if(!started) 
			return;
		
		if (keepLivenessTicker != null) {
			keepLivenessTicker.cancel(false);
			keepLivenessTicker = null;
		}
		
		clientEventEngine.stop();
		started = false;
	}
	
	protected synchronized int quorum() {
		return clientEventEngine.peerCnt() / 2 + 1;
	}
	
	protected synchronized void updateReplicaVersion(long replicaUuid, long replicaVersion) {
		if (replicaVersion > maxReplicaVersion) {
			maxReplicaVersion = replicaVersion;
			maxReplicaVersionReplicaSet.clear();
			maxReplicaVersionReplicaSet.add(replicaUuid);
			logger.debug("[{}] maxReplicaVersion update to {}", uuid, replicaVersion);
		} else if (replicaVersion == maxReplicaVersion) {
			maxReplicaVersionReplicaSet.add(replicaUuid);
		}
	}
	
	public synchronized long uuid() {
		return uuid;
	}
	
	@Override
	public synchronized void onOutboundUnconnectedOrError(long uuid) {
		
	}
	
	@Override
	public synchronized void onInboundUnconnectedOrError(ClientConnection cch) {
		
	}
	
	@SuppressWarnings("unchecked")
	protected synchronized void onClientResp(ClientResp resp) {
		if (logger.isDebugEnabled()) logger.debug("[{}] onClientResp, resp={}, firstReqCtx={}, getLeaderCtx={}", 
				uuid(), resp, reqCtxQueue.peekFirst(), getLeaderCtx);
		
		updateReplicaVersion(resp.srcUuid, resp.result.replicaVersion);
		
		RequestContext reqCtx = reqCtxQueue.peekFirst();
		if (reqCtx == null)
			return;
		
		if (reqCtx.luid != resp.luid) 
			return;
		
		if (resp.result instanceof PaxOperationResultHandler) {
			PaxOperationResultHandler result = (PaxOperationResultHandler)resp.result;
			if (!result.success) {
				((Future<Object>)reqCtx.future).finish(result.success, result.errorMsg, null);
				return;
			}
			
			ClientReq req = (ClientReq)reqCtx.req;
			if (req.cmd instanceof PaxOperationHandlerOpen) {
				PaxOperationHandlerOpen op = (PaxOperationHandlerOpen)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle != null) {
					handle.setData(result.data);
					handle.setDataVersoin(result.dataVersion);
					handle.setLockHeld(result.lockHeld);
					((Future<Handle>)reqCtx.future).finish(true, null, handle);
				} else {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
				}
			} else if (req.cmd instanceof PaxOperationHandlerRead) {
				PaxOperationHandlerRead op = (PaxOperationHandlerRead)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle != null) {
					handle.setData(result.data);
					handle.setDataVersoin(result.dataVersion);
					((Future<byte[]>)reqCtx.future).finish(true, null, handle.data);
				} else {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
				}
			} else if (req.cmd instanceof PaxOperationHandlerWrite) {
				PaxOperationHandlerWrite op = (PaxOperationHandlerWrite)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle != null) {
					handle.setData(op.data);
					handle.setDataVersoin(result.dataVersion);
					((Future<Long>)reqCtx.future).finish(true, null, handle.dataVersion);	
				} else {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
				}
			} else if (req.cmd instanceof PaxOperationHandlerLock) {
				PaxOperationHandlerLock op = (PaxOperationHandlerLock)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle != null) {
					handle.setLockHeld(true);
					((Future<Boolean>)reqCtx.future).finish(true, null, true);
				} else {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
				}
			} else if (req.cmd instanceof PaxOperationHandlerClose) {
				//do nothing
				PaxOperationHandlerClose op = (PaxOperationHandlerClose)req.cmd;
				handleMap.remove(op.fd);
			}
		} else if (resp.result instanceof PaxOperationResultUpdate) {
			PaxOperationResultUpdate result = (PaxOperationResultUpdate)resp.result;
			((Future<Long>)reqCtx.future).finish(result.errorMsg == null, result.errorMsg, result.replicaVersion);
		} else if (resp.result instanceof PaxOperationResultQuery) {
			PaxOperationResultQuery result = (PaxOperationResultQuery)resp.result;
			((Future<Object>)reqCtx.future).finish(result.errorMsg == null, result.errorMsg, result.object);
		} else if (resp.result instanceof PaxOperationResultErrorLeader) {
			reqCtx.running = false;
			reqCtx.tryCnt = 0;
			getLeaderCtx.start();
			return;
		}
		
		reqCtxQueue.pollFirst();
	}
	
	@SuppressWarnings("unchecked")
	protected synchronized void onClientApplyForUuidResp(ClientApplyForUuidResp resp) {
		if (logger.isDebugEnabled()) logger.debug("[{}] onClientApplyForUuidResp, resp={}, firstReqCtx={}", 
				uuid(), resp, reqCtxQueue.peekFirst());
		
		updateReplicaVersion(resp.srcUuid, resp.replicaVersion);
		
		RequestContext reqCtx = reqCtxQueue.peekFirst();
		while (reqCtx != null && reqCtx.req instanceof ClientApplyForUuidReq) {
			if (uuid == 0) {
				uuid = resp.clientUuid;
				
				if (logger.isDebugEnabled()) logger.debug("[{}] uuid established, uuid={}", uuid(), uuid());
				
				if (reqCtx.future != null)
					((Future<Long>)reqCtx.future).finish(true, null, uuid);
				
				//send get leader info request
				getLeaderCtx.start();
			}
			reqCtxQueue.pollFirst();
			reqCtx = reqCtxQueue.peekFirst();
		}
	}
	
	public synchronized void onClientGetLeaderResp(ClientGetLeaderResp resp) {
		getLeaderCtx.onResp(resp);
	}
	
	
	public long handleEventLatestReplicaVersion = 0;
	
	public synchronized void onEventNotify(EventNotify noti) {
		if (logger.isDebugEnabled()) logger.debug("[{}] onEventNotify, noti={}", uuid, noti);
		
		if (noti.replicaVersion <= handleEventLatestReplicaVersion)
			return;
		
		handleEventLatestReplicaVersion = noti.replicaVersion;
		
		if (noti.event == null)
			return;
		
		if (noti.event instanceof HandleEvent) {
			HandleEvent event = (HandleEvent)noti.event;
			
			HandleImpl handle = handleMap.get(event.fd);
			if (handle == null)
				logger.warn("[{}] recv handle event, handle not found, noti={}", uuid(), noti);
			else
				handle.offerHandlerEventNotify(event);
		}
	}
	
	@Override
	public synchronized void onMessage(Message msg) {
		switch (msg.id) {
		case MessageId.PAX_CLIENT_RESP:
			onClientResp((ClientResp)msg);
			break;
		case MessageId.PAX_CLIENT_APPLY_FOR_UUID_RESP:
			onClientApplyForUuidResp((ClientApplyForUuidResp)msg);
			break;
		case MessageId.PAX_CLIENT_GET_LEADER_RESP:
			onClientGetLeaderResp((ClientGetLeaderResp)msg);
			break;
		case MessageId.PAX_EVENT_NOTIFY:
			onEventNotify((EventNotify)msg);
			break;
		default:
			logger.warn("[{}] unkown message: {}", uuid, msg.id);
			break;
		}
		
		executeNextRequest();
	}
	
	protected synchronized void executeNextRequest() {
		if (getLeaderCtx.isRunning())
			return;
		
		if (!getLeaderCtx.isEstablished())
			return;
			
		RequestContext reqCtx = reqCtxQueue.peekFirst();
		if (reqCtx == null)
			return;
		
		if (reqCtx.running)
			return;
		
		
		
		reqCtx.running = true;
		reqCtx.tryCnt++;
		
		if (reqCtx.req instanceof ClientReq) {
			((ClientReq)reqCtx.req).leaderInfo = getLeaderCtx.getLeaderInfo();
		}
		
		messager.send(getLeaderCtx.getLeaderInfo().uuid, reqCtx.req);
	}
	
	public synchronized Future<Long> applyForUuid() throws Exception {
		DefaultFuture<Long> future = new DefaultFuture<>();
		
		if (uuid != 0) {
			future.finish(true, null, uuid);
			return future;
		}
		
		if (reqCtxQueue.size() != 0)
			throw new PaxClientException("reqCtxQueue.size > 0");
		
		long luid = genIsn();
		Message req = new ClientApplyForUuidReq();
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);
		
		messager.broadcast(new ClientApplyForUuidReq());
		
		if (logger.isDebugEnabled()) logger.debug("applyForUuid start");
		
		return future;
	}
	
	/**
	 * @param expectedVersion
	 * 		if the key version on the replica is not equal to {@code expectedKeyVersion}, 
	 * 		do not commit the put command. If expectedKeyVersion is -1, replica will not
	 * 		check the version.
	 */
	public synchronized Future<Long> put(String key, byte[] value, long expectedKeyVersion)  throws PaxClientUuidIsNullException {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		DefaultFuture<Long> future = new DefaultFuture<>();
		long luid = genIsn();		
		Command cmd = new PaxOperationKV(uuid, luid, PaxOperationKV.TYPE_PUT, key, value, expectedKeyVersion);	
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);	
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);
		
		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Object> get(String key) throws PaxClientUuidIsNullException {	
		if (uuid == 0)
			throw new PaxClientUuidIsNullException();
		
		DefaultFuture<Object> future = new DefaultFuture<>();
		long luid = genIsn();
		Command cmd = new PaxOperationKV(uuid, luid, PaxOperationKV.TYPE_GET, key);
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);
		
		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Void> createNode(String path, byte[] value) throws Exception {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<Void> future = new DefaultFuture<>();
		Command cmd = new PaxOperationNodeCreate(uuid, luid, path, value, -1, -1);
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);
		
		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Void> deleteNode(String path) throws Exception {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<Void> future = new DefaultFuture<>();
		Command cmd = new PaxOperationNode(uuid, luid, PaxOperationNode.DELETE, path);
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);
		
		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Handle> open(String path, byte[] data, int flag) throws Exception {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<Handle> future = new DefaultFuture<>();
		
		HandleFD fd = new HandleFD(uuid, luid);
		HandleImpl handle = new HandleImpl(fd, path, ((flag & HandlerFlag.EPHEMERAL) != 0));
		handleMap.put(fd, handle);
		
		Command cmd = new PaxOperationHandlerOpen(uuid, luid, fd, path, data, flag);
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<byte[]> read(Handle handle) throws Exception {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<byte[]> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerRead(uuid, luid, handle.fd());
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Long> write(Handle handle, byte[] data) throws Exception {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<Long> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerWrite(uuid, luid, handle.fd(), data);
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Void> close(Handle handle) throws Exception {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<Void> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerClose(uuid, luid, handle.fd());
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Boolean> lock(Handle handle) throws Exception {
		if (uuid == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<Boolean> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerLock(uuid, luid, handle.fd());
		Message req = new ClientReq(uuid, getLeaderCtx.getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		reqCtxQueue.add(reqCtx);

		executeNextRequest();
		
		return future;
	}
}

