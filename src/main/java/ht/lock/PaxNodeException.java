package ht.lock;

import ht.pax.internal.exception.PaxInternalException;

public class PaxNodeException extends PaxInternalException {
	private static final long serialVersionUID = -763618247875552004L;
	
	public PaxNodeException() {
		super();
	}
	
	public PaxNodeException(String message) {
		super(message);
	}
	
	public PaxNodeException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public PaxNodeException(Throwable cause) {
        super(cause);
    }
}
