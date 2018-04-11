package cassdoc.exceptions

import groovy.transform.CompileStatic

@CompileStatic
class InterruptQueryException extends RuntimeException {
    private static final long serialVersionUID = -989546846467L

    InterruptQueryException() {
    }

    InterruptQueryException(String var1) {
        super(var1)
    }

    InterruptQueryException(String var1, Throwable var2) {
        super(var1, var2)
    }

    InterruptQueryException(Throwable var1) {
        super(var1)
    }
}
