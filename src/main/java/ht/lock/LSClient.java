package ht.lock;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ht.pax.client.PaxClient;
import ht.pax.client.PaxClientException;
import ht.pax.client.PaxClientUuidIsNullException;
import ht.pax.common.Command;
import ht.pax.common.Config;
import ht.pax.common.DefaultFuture;
import ht.pax.common.Future;
import ht.pax.common.Message;
import ht.pax.common.PaxOperationResult;
import ht.pax.internal.message.ClientReq;
import ht.pax.internal.message.EventNotify;

public class LSClient extends PaxClient {
	private static Logger logger = LoggerFactory.getLogger(LSClient.class);
	
	HashMap<HandleFD, HandleImpl> handleMap = new HashMap<HandleFD, HandleImpl>();
	
	enum HandleState {
		Opened,
		Closed,
		Opening
	}
	
	public class HandleImpl implements Handle {
		
		final HandleFD fd;
		final String path;
		boolean ephemeral;
		boolean lockHeld;
		byte[] data;
		long dataVersion;
		
		
		HandleState state;
		ConcurrentLinkedQueue<HandleEvent> handleEventQueue = new ConcurrentLinkedQueue<>();
		
		public HandleImpl(HandleFD fd, String path, boolean ephemeral) {
			this.fd = fd;
			this.path = path;
			this.ephemeral = ephemeral;
			this.state = HandleState.Closed;
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
			synchronized(LSClient.this) {
				return ephemeral;
			}
		}
		
		public void setEphemeral(boolean b) {
			synchronized(LSClient.this) {
				ephemeral = b;
			}
		}
		
		@Override
		public boolean isLockHeld() {
			synchronized(LSClient.this) {
				return lockHeld;
			}
		}
		
		public void setLockHeld(boolean b) {
			synchronized(LSClient.this) {
				lockHeld = b;
			}
		}
		
		@Override
		public byte[] data() {
			synchronized(LSClient.this) {
				return data;
			}
		}
		
		public void setData(byte[] data) {
			synchronized(LSClient.this) {
				this.data = data;
			}
		}
		
		@Override
		public long dataVersion() {
			synchronized(LSClient.this) {
				return dataVersion;
			}
		}
		
		public void setDataVersoin(long dataVersion) {
			synchronized(LSClient.this) {
				this.dataVersion = dataVersion;
			}
		}
		
		public boolean isClosed() {
			synchronized(LSClient.this) {
				return state == HandleState.Closed;
			}
		}
		
		public void setClosed() {
			synchronized(LSClient.this) {
				state = HandleState.Closed;
			}
		}
		
		public boolean isOpened() {
			synchronized(LSClient.this) {
				return state == HandleState.Opened;
			}
		}
		
		public void setOpened() {
			synchronized(LSClient.this) {
				state = HandleState.Opened;
			}
		}
		
		public boolean isOpening() {
			return state == HandleState.Opening;
		}
		
		public void setOpening() {
			state = HandleState.Opening;
		}
		
		public void offerHandlerEventNotify(HandleEvent event) {
			if (!isOpened())
				return;
			handleEventQueue.offer(event);
		}
		
		@Override
		public HandleEvent pollHandlerEventNotify() {
			if (!isOpened())
				return null;
			return handleEventQueue.poll();
		}
		
		@Override
		public byte[] read() throws Exception {
			Future<byte[]> f = LSClient.this.readAsync(this);
			f.sync();
			if (!f.isSuccess())
				throw new PaxClientException(f.errorMsg());
			return f.get();
		}
		
		@Override
		public long write(byte[] data) throws Exception {
			Future<Long> f = LSClient.this.writeAsync(this, data);
			f.sync();
			if (!f.isSuccess())
				throw new PaxClientException(f.errorMsg());
			return f.get();
		}
		
		@Override
		public boolean lock() throws Exception {
			Future<Boolean> f = LSClient.this.lockAsync(this);
			f.sync();
			if (!f.isSuccess())
				throw new PaxClientException(f.errorMsg());
			return f.get();
		}
		
		@Override
		public void close() throws Exception {
			Future<Void> f = LSClient.this.closeAsync(this);
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
	
	public LSClient(Config config) {
		super(config);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected void onOperatorResult(PaxOperationResult result0, RequestContext reqCtx) {
		if (result0 instanceof PaxOperationResultHandle) {
			PaxOperationResultHandle result = (PaxOperationResultHandle)result0;			
			ClientReq req = (ClientReq)reqCtx.req;
			if (req.cmd instanceof PaxOperationHandleOpen) {
				PaxOperationHandleOpen op = (PaxOperationHandleOpen)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle == null) {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
					return;
				}
				
				if (result.success) {
					handle.setData(result.data);
					handle.setDataVersoin(result.dataVersion);
					handle.setLockHeld(result.lockHeld);
					handle.setOpened();
					((Future<Handle>)reqCtx.future).finish(true, null, handle);
				} else {
					handle.setClosed();
					((Future<Object>)reqCtx.future).finish(false, result.errorMsg, null);
				}
			} else if (req.cmd instanceof PaxOperationHandleRead) {
				PaxOperationHandleRead op = (PaxOperationHandleRead)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle == null) {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
					return;
				}
				
				if (result.success) {
					handle.setData(result.data);
					handle.setDataVersoin(result.dataVersion);
					((Future<byte[]>)reqCtx.future).finish(true, null, handle.data);
				} else {
					//TODO: should closed in some cases.
					((Future<Object>)reqCtx.future).finish(false, result.errorMsg, null);
				}
			} else if (req.cmd instanceof PaxOperationHandleWrite) {
				PaxOperationHandleWrite op = (PaxOperationHandleWrite)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle == null) {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
					return;
				}
				
				if (result.success) {
					handle.setData(op.data);
					handle.setDataVersoin(result.dataVersion);
					((Future<Long>)reqCtx.future).finish(true, null, handle.dataVersion);	
				} else {
					//TODO: should closed in some cases.
					((Future<Object>)reqCtx.future).finish(false, result.errorMsg, null);
				}
			} else if (req.cmd instanceof PaxOperationHandleLock) {
				PaxOperationHandleLock op = (PaxOperationHandleLock)req.cmd;
				HandleImpl handle = handleMap.get(op.fd);
				if (handle == null) {
					logger.warn("[{}] handle not fould, result={}", uuid(), result);
					return;
				}
				
				if (result.success) {
					handle.setLockHeld(true);
					((Future<Boolean>)reqCtx.future).finish(true, null, true);
				} else {
					//TODO: should closed in some cases.
					((Future<Object>)reqCtx.future).finish(false, result.errorMsg, null);
				}
			} else if (req.cmd instanceof PaxOperationHandleClose) {
				//do nothing
				PaxOperationHandleClose op = (PaxOperationHandleClose)req.cmd;
				HandleImpl handle = handleMap.remove(op.fd);
				handle.setClosed();
				((Future<Void>)reqCtx.future).finish(true, null, null);
			} else {
				//TODO:
			}
		} else if (result0 instanceof PaxOperationResultNodeQuery) {
			PaxOperationResultNodeQuery result = (PaxOperationResultNodeQuery)result0;
			ClientReq req = (ClientReq)reqCtx.req;
			if (req.cmd instanceof PaxOperationNode) {
				if (result0.success) {
					((Future<NodeInfo>)reqCtx.future).finish(true, null,result.nodeInfo);
				} else {
					((Future<NodeInfo>)reqCtx.future).finish(false, result.errorMsg, null);
				}
			} else {
				//TODO
			}
		} else if (result0 instanceof PaxOperationResultNodeUpdate) {
			ClientReq req = (ClientReq)reqCtx.req;
			if (req.cmd instanceof PaxOperationNodeCreate) {
				NodeInfo ni = (NodeInfo)reqCtx.arg;
				if (result0.success)
					((Future<NodeInfo>)reqCtx.future).finish(true, null, ni);
				else
					((Future<NodeInfo>)reqCtx.future).finish(false, result0.errorMsg, null);
			} else if (req.cmd instanceof PaxOperationNode) {
				((Future<Void>)reqCtx.future).finish(result0.success, result0.errorMsg, null);
			} else {
				//TODO
			}
		}
	}
	
	@Override
	protected void onEventNotifyInternal(EventNotify noti) {
		if (noti.event instanceof HandleEvent) {
			HandleEvent event = (HandleEvent)noti.event;
			
			HandleImpl handle = handleMap.get(event.fd);
			if (handle == null)
				logger.warn("[{}] recv handle event, handle not found, noti={}", uuid(), noti);
			else
				handle.offerHandlerEventNotify(event);
		}
	}
	
	public synchronized Future<NodeInfo> createNodeAsync(String path, byte[] data) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<NodeInfo> future = new DefaultFuture<>();
		Command cmd = new PaxOperationNodeCreate(uuid(), luid, path, data, -1, -1);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		NodeInfo ni = new NodeInfo();
		ni.data = data;
		ni.dataVersion = 1;
		ni.treeVersion = 1;
		RequestContext reqCtx = new RequestContext(req, luid, future, ni);
		addReqCtx(reqCtx);
		
		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Void> deleteNodeAsync(String path) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<Void> future = new DefaultFuture<>();
		Command cmd = new PaxOperationNode(uuid(), luid, PaxOperationNode.DELETE, path);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);
		
		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<NodeInfo> queryNode(String path, byte type) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<NodeInfo> future = new DefaultFuture<>();
		Command cmd = new PaxOperationNode(uuid(), luid, type, path);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);
		
		executeNextRequest();
		
		return future;
	}
	
	
	public synchronized Future<Handle> openAsync(String path, byte[] data, int flag) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<Handle> future = new DefaultFuture<>();
		
		HandleFD fd = new HandleFD(uuid(), luid);
		HandleImpl handle = new HandleImpl(fd, path, ((flag & HandleFlag.EPHEMERAL) != 0));
		handle.setOpening();
		handleMap.put(fd, handle);
		
		Command cmd = new PaxOperationHandleOpen(uuid(), luid, fd, path, data, flag);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<byte[]> readAsync(Handle handle) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		if (!handleMap.containsKey(handle.fd())) throw new PaxClientException("fd not fould");
		if (!handle.isOpened()) throw new PaxClientException("handle not opened");
		
		long luid = genIsn();
		DefaultFuture<byte[]> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandleRead(uuid(), luid, handle.fd());
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Long> writeAsync(Handle handle, byte[] data) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		if (!handleMap.containsKey(handle.fd())) throw new PaxClientException("fd not fould");	
		if (!handle.isOpened()) throw new PaxClientException("handle not opened");
		
		long luid = genIsn();
		DefaultFuture<Long> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandleWrite(uuid(), luid, handle.fd(), data);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Void> closeAsync(Handle handle) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		if (!handleMap.containsKey(handle.fd())) throw new PaxClientException("fd not fould");
		if (!handle.isOpened()) throw new PaxClientException("handle not opened");
		
		long luid = genIsn();
		DefaultFuture<Void> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandleClose(uuid(), luid, handle.fd());
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Boolean> lockAsync(Handle handle) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		if (!handleMap.containsKey(handle.fd())) throw new PaxClientException("fd not fould");
		if (!handle.isOpened()) throw new PaxClientException("handle not opened");
		
		long luid = genIsn();
		DefaultFuture<Boolean> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandleLock(uuid(), luid, handle.fd());
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	/*
	 * call by user
	 */
	
	public Handle open(String path, byte[] data, int flag) throws Exception {
		Future<Handle> f = openAsync(path, data, flag);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
	
	public NodeInfo createNode(String path, byte[] value) throws Exception {
		Future<NodeInfo> f = createNodeAsync(path, value);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
	
	public boolean deleteNode(String path) throws Exception {
		Future<Void> f = deleteNodeAsync(path);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return true;
	}
	
	public NodeInfo queryNode(String path) throws Exception {
		Future<NodeInfo> f = queryNode(path, PaxOperationNode.QUERY_ALL);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
	
	public NodeInfo queryNodeData(String path) throws Exception {
		Future<NodeInfo> f = queryNode(path, PaxOperationNode.QUERY_DATA);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
	
	public NodeInfo queryNodeChildren(String path) throws Exception {
		Future<NodeInfo> f = queryNode(path, PaxOperationNode.QUERY_CHILDREN);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
}
