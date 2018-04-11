package cassdoc.exceptions

import groovy.transform.CompileStatic

/**
 * Whenever type information for provided data doesn't line up with metadata and behavioral configurations...
 *
 * @author a999166
 */

@CompileStatic
class InvalidTypeException extends IllegalStateException {
    private static final long serialVersionUID = 658979438789573L

    InvalidTypeException() {
    }

    InvalidTypeException(String message) {
        super(message)
    }

    InvalidTypeException(Throwable cause) {
        super(cause)
    }

    InvalidTypeException(String message, Throwable cause) {
        super(message, cause)
    }
}
