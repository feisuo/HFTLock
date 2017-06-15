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
import ht.pax.common.HandleEvent;
import ht.pax.common.HandleFD;
import ht.pax.common.HandlerFlag;
import ht.pax.common.Message;
import ht.pax.common.PaxOperationResult;
import ht.pax.internal.message.ClientReq;
import ht.pax.internal.message.EventNotify;

public class LSClient extends PaxClient {
	private static Logger logger = LoggerFactory.getLogger(LSClient.class);
	
	HashMap<HandleFD, HandleImpl> handleMap = new HashMap<HandleFD, HandleImpl>();
	
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
		
		public void offerHandlerEventNotify(HandleEvent event) {
			handleEventQueue.offer(event);
		}
		
		@Override
		public HandleEvent pollHandlerEventNotify() {
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
		if (result0 instanceof PaxOperationResultHandler) {
			PaxOperationResultHandler result = (PaxOperationResultHandler)result0;
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
	
	public synchronized Future<Void> createNodeAsync(String path, byte[] value) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<Void> future = new DefaultFuture<>();
		Command cmd = new PaxOperationNodeCreate(uuid(), luid, path, value, -1, -1);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
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
	
	public synchronized Future<Handle> openAsync(String path, byte[] data, int flag) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		long luid = genIsn();
		
		DefaultFuture<Handle> future = new DefaultFuture<>();
		
		HandleFD fd = new HandleFD(uuid(), luid);
		HandleImpl handle = new HandleImpl(fd, path, ((flag & HandlerFlag.EPHEMERAL) != 0));
		handleMap.put(fd, handle);
		
		Command cmd = new PaxOperationHandlerOpen(uuid(), luid, fd, path, data, flag);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<byte[]> readAsync(Handle handle) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<byte[]> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerRead(uuid(), luid, handle.fd());
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Long> writeAsync(Handle handle, byte[] data) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<Long> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerWrite(uuid(), luid, handle.fd(), data);
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Void> closeAsync(Handle handle) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<Void> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerClose(uuid(), luid, handle.fd());
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	public synchronized Future<Boolean> lockAsync(Handle handle) throws Exception {
		if (uuid() == 0) throw new PaxClientUuidIsNullException();
		
		if (!handleMap.containsKey(handle.fd()))
			throw new PaxClientException("fd not fould");
		
		long luid = genIsn();
		
		DefaultFuture<Boolean> future = new DefaultFuture<>();
		Command cmd = new PaxOperationHandlerLock(uuid(), luid, handle.fd());
		Message req = new ClientReq(uuid(), getLeaderInfo(), cmd);
		RequestContext reqCtx = new RequestContext(req, luid, future);
		addReqCtx(reqCtx);

		executeNextRequest();
		
		return future;
	}
	
	
	public Handle open(String path, byte[] data, int flag) throws Exception {
		Future<Handle> f = openAsync(path, data, flag);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return f.get();
	}
	
	public boolean createNode(String path, byte[] value) throws Exception {
		Future<Void> f = createNodeAsync(path, value);
		f.sync();
		if (!f.isSuccess())
			throw new PaxClientException(f.errorMsg());
		return true;
	}
}
