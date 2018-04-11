package cassdoc.exceptions

import groovy.transform.CompileStatic

@CompileStatic
class UnexpectedPersistenceStateException extends IllegalStateException {
    private static final long serialVersionUID = 3289774963321789573L

    UnexpectedPersistenceStateException() {
    }

    UnexpectedPersistenceStateException(String message) {
        super(message)
    }

    UnexpectedPersistenceStateException(Throwable cause) {
        super(cause)
    }

    UnexpectedPersistenceStateException(String message, Throwable cause) {
        super(message, cause)
    }
}
