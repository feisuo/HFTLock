package com.tchaicatkovsky.lock;

import com.tchaicatkovsky.pax.internal.exception.PaxInternalException;

public class NodeException extends PaxInternalException {
	private static final long serialVersionUID = -763618247875562004L; //NodeException
	
	public NodeException() {
		super();
	}
	
	public NodeException(String message) {
		super(message);
	}
	
	public NodeException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public NodeException(Throwable cause) {
        super(cause);
    }
}
