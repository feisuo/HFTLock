package ht.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ht.pax.api.CellService;
import ht.pax.api.SMRService;
import ht.pax.common.ClientConnection;
import ht.pax.common.Command;
import ht.pax.common.HandleEvent;
import ht.pax.common.HandleFD;
import ht.pax.common.HandlerFlag;
import ht.pax.common.LeaderInfo;
import ht.pax.common.PaxOperationResult;
import ht.pax.internal.message.EventNotify;

public class CellLockService implements CellService {

	private static Logger logger = LoggerFactory.getLogger(CellLockService.class);
	
	final HandleManager handlerManager;
	SMRService smr;
	long uuid;
	
	public CellLockService() {
		this.handlerManager = new HandleManager();
	}
	
	long uuid() {
		return uuid;
	}
	
	@Override
	public void setSRMService(SMRService service) {
		this.smr = service;
		this.uuid = smr.uuid();
	}

	@Override
	public boolean onRequest(Command cmd) {
		if (cmd instanceof PaxOperationHandlerRead) {
			PaxOperationHandlerRead op = (PaxOperationHandlerRead)cmd;
			PaxOperationResultHandler result = new PaxOperationResultHandler();
			result.replicaVersion = smr.version();
			HandleContext ctx = handlerManager.getContext(op.fd);
			if (ctx == null) {
				result.success = false;
				result.errorMsg = "null ctx";
			} else {
				result.data = ctx.node().value;
				result.dataVersion = ctx.node().valueVersion;
				result.success = true;
			}
			smr.respCmd(cmd, smr.version(), result);
			return false;
		}
		
		return true;
	}

	@Override
	public PaxOperationResult onCommand(long iid, Command cmd) {
		PaxOperationResult result0 = null;
		
		if (cmd instanceof PaxOperationHandlerOpen) {
			PaxOperationHandlerOpen op = (PaxOperationHandlerOpen)cmd;
			PaxOperationResultHandler result = new PaxOperationResultHandler();
			result.replicaVersion = iid;
			boolean ephemeral = ((op.flag & HandlerFlag.EPHEMERAL) != 0);
			boolean tryLock = ((op.flag & HandlerFlag.TRY_LOCK) != 0);
			try {
				handlerManager.open(op.fd, op.path, op.data, ephemeral, tryLock);
				HandleContext ctx = handlerManager.getContext(op.fd);
				result.success = true;
				result.lockHeld = ctx.isLockHeld();
				result.data = ctx.data();
				result.dataVersion = ctx.dataVersion();
			} catch (Exception e) {
				if(e.getMessage() != null && e.getMessage().contains("held")) {
					HandleContext ctx = handlerManager.getContext(op.fd);
					result.success = true;
					result.lockHeld = ctx.isLockHeld();
					result.data = ctx.data();
					result.dataVersion = ctx.dataVersion();
				} else {
					result.success = false;
					result.errorMsg = e.getMessage();
				}
			}
			
			result0 = result;
		} else if (cmd instanceof PaxOperationHandlerWrite) {
			PaxOperationHandlerWrite op = (PaxOperationHandlerWrite)cmd;
			PaxOperationResultHandler result = new PaxOperationResultHandler();
			result.replicaVersion = iid;
			try {
				long dataVersion = handlerManager.write(op.fd, op.data);
				result.success = true;
				result.dataVersion = dataVersion;
			} catch (Exception e) {
				result.success = false;
				result.errorMsg = e.getMessage();
			}
			
			result0 = result;
		} else if (cmd instanceof PaxOperationHandlerClose) {
			PaxOperationHandlerClose op = (PaxOperationHandlerClose)cmd;
			PaxOperationResultHandler result = new PaxOperationResultHandler();
			result.replicaVersion = iid;
			result.success = true;
			
			HandleContext ctx = handlerManager.close(op.fd);
			if (ctx != null && ctx.isLockHeld()) {
				notifyClient(ctx);
			}
			
			result0 = result;
		} else if (cmd instanceof PaxOperationHandlerLock) {
			PaxOperationHandlerLock op = (PaxOperationHandlerLock)cmd;
			PaxOperationResultHandler result = new PaxOperationResultHandler();
			result.replicaVersion = iid;
			try {
				result.success = handlerManager.lock(op.fd);
			} catch (Exception e) {
				result.success = false;
				result.errorMsg = e.getMessage();
			}
			
			result0 = result;
		}

		return result0;
	}

	@Override
	public void onLeaderChanged(LeaderInfo newLeader) {

	}

	@Override
	public void onClientLoss(ClientConnection cc) {
		List<HandleFD> fdList = new ArrayList<>();
		
		List<HandleContext> l = handlerManager.close(cc.getUuid());
		if (l != null) {
			for (HandleContext ctx : l) {
				if (ctx.isLockHeld()) {
					notifyClient(ctx);
					if (logger.isDebugEnabled())
						fdList.add(ctx.fd());
				}
			}
		}
		
		if (logger.isDebugEnabled()) {
			int ctxCnt = (l == null ? 0 : l.size());
			logger.debug("[{}] onClientLoss, clientUuid={}, ctxCnt={}, lockHeldFDList={}", uuid(), cc.getUuid(), ctxCnt, fdList);
		}
	}

	@Override
	public String getSummaryInfo() {
		return null;
	}

	protected void notifyClient(HandleContext ctx) {
		if (!smr.isLeader())
			return;
		
		Map<HandleFD, HandleContext> watcherList = handlerManager.getWatherList(ctx.path());
		if (watcherList != null) {
			
			for (HandleContext watcher : watcherList.values()) {
				HandleEvent event = new HandleEvent(watcher.fd());
				event.setUnlock();
				EventNotify noti = new EventNotify(smr.uuid(), event, smr.version());
				smr.sendToClient(watcher.fd().uuid, noti);
			}
		}
	}
}
