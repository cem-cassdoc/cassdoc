package cassdoc.exceptions

import groovy.transform.CompileStatic

/**
 * For when CQL detects something already exists and that wasn't expected (create, etc).
 *
 * @author a999166
 */

@CompileStatic
class ResourceNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 3289794338789713L

    ResourceNotFoundException() {
    }

    ResourceNotFoundException(String message) {
        super(message)
    }

    ResourceNotFoundException(Throwable cause) {
        super(cause)
    }

    ResourceNotFoundException(String message, Throwable cause) {
        super(message, cause)
    }
}
