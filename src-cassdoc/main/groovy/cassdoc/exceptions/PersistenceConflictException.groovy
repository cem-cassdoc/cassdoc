package cassdoc.exceptions

import groovy.transform.CompileStatic

/**
 * General error in persistence occurred
 *
 * @author a999166
 */

@CompileStatic
class PersistenceConflictException extends PersistenceException {
    private static final long serialVersionUID = 321976438729576L

    PersistenceConflictException() {
    }

    PersistenceConflictException(String message) {
        super(message)
    }

    PersistenceConflictException(Throwable cause) {
        super(cause)
    }

    PersistenceConflictException(String message, Throwable cause) {
        super(message, cause)
    }
}
