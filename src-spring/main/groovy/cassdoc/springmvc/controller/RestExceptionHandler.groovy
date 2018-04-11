package cassdoc.springmvc.controller

import cassdoc.exceptions.DuplicateResourceException
import cassdoc.exceptions.PersistenceException
import cassdoc.exceptions.ResourceNotFoundException
import cwdrg.lg.annotation.Log
import groovy.transform.CompileStatic
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.context.request.WebRequest
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler

@ControllerAdvice
@CompileStatic
@Log
class RestExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(value = [ DuplicateResourceException, IllegalStateException, PersistenceException ])
    protected ResponseEntity<Object> handleConflict(RuntimeException ex, WebRequest request) {
        log.inf("HANDLING CONFLICT EXCEPTION",ex)
        String bodyOfResponse = "Conflict"
        return handleExceptionInternal(ex, bodyOfResponse, new HttpHeaders(), HttpStatus.CONFLICT, request)
    }

    @ExceptionHandler(value = [ ResourceNotFoundException, IllegalArgumentException ])
    protected ResponseEntity<Object> handleNotFound(RuntimeException ex, WebRequest request) {
        log.inf("HANDLING NOTFOUND EXCEPTION",ex)
        String bodyOfResponse = "Not Found"
        return handleExceptionInternal(ex, bodyOfResponse, new HttpHeaders(), HttpStatus.NOT_FOUND, request)
    }

}
