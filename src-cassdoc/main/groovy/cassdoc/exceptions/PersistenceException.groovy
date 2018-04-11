package cassdoc.exceptions

import groovy.transform.CompileStatic

/**
 * General error in persistence occurred
 *
 * @author a999166
 */

@CompileStatic
class PersistenceException extends RuntimeException {
    private static final long serialVersionUID = 321976438729576L

    PersistenceException() {
    }

    PersistenceException(String message) {
        super(message)
    }

    PersistenceException(Throwable cause) {
        super(cause)
    }

    PersistenceException(String message, Throwable cause) {
        super(message, cause)
    }
}
