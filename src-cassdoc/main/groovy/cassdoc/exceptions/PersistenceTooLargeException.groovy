package cassdoc.exceptions

import groovy.transform.CompileStatic

/**
 * General error in persistence occurred
 *
 * @author a999166
 */

@CompileStatic
class PersistenceTooLargeException extends PersistenceException {
    private static final long serialVersionUID = 321976438729576L

    PersistenceTooLargeException() {
    }

    PersistenceTooLargeException(String message) {
        super(message)
    }

    PersistenceTooLargeException(Throwable cause) {
        super(cause)
    }

    PersistenceTooLargeException(String message, Throwable cause) {
        super(message, cause)
    }
}
