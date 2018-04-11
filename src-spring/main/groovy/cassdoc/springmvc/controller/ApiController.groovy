package cassdoc.springmvc.controller

import cassdoc.CassdocAPI
import cassdoc.Detail
import cassdoc.springmvc.service.CtxDtl
import cassdoc.springmvc.service.PrepareCtx
import cwdrg.lg.annotation.Log
import cwdrg.spring.annotation.RequestParamJSON
import cwdrg.util.json.JSONUtil
import groovy.transform.CompileStatic
import org.apache.commons.io.IOUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

import javax.servlet.ServletInputStream
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@Log
@RestController
@CompileStatic
class ApiController {
    @Autowired
    PrepareCtx prepareCtx

    @Autowired
    CassdocAPI api

    // CEM: the injection of the HttpServletRequest and/or Response has been causing @RequestParam mapped
    // method parameters to result in ambiguous mapping errors in the integration test. Works if we no longer
    // use those annotations and user the HttpRequest.getParameter

    @RequestMapping(value = '/up', method = RequestMethod.GET)
    String status() {
        log.inf('GET /up --> status()', null)
        return '{"webappStatus":"up"}'
    }

    @RequestMapping(value = '/doc/{collection}/{id}/', method = RequestMethod.HEAD)
    boolean docExists(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            @RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON
    ) {
        log.inf("HEAD /doc/$collection/$uuid --> docExists()", null)
        CtxDtl ctxDtl = prepareCtx.readOnlyCtxDtl(collection, customDetailJSON)
        api.docExists(ctxDtl.ctx, ctxDtl.dtl, uuid)
    }

    @RequestMapping(value = '/doc/{collection}/{id}/{attr}', method = RequestMethod.HEAD)
    boolean attrExists(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            @PathVariable(value = 'attr', required = true) String attr,
            @RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON
    ) {
        log.inf("HEAD /doc/$collection/$uuid/$attr --> attrExists()", null)
        CtxDtl ctxDtl = prepareCtx.readOnlyCtxDtl(collection, customDetailJSON)
        api.attrExists(ctxDtl.ctx, ctxDtl.dtl, uuid, attr)
    }

    @RequestMapping(value = '/doc/{collection}/{id}', method = RequestMethod.GET)
    void retrieveDoc(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            //@RequestParam(value = 'simple', required = false) Boolean simple = false,
            //@RequestParam(value = 'jsonPath', required = false) String jsonPath = null,
            //@RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON,
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        log.inf("GET /doc/$collection/$uuid --> retrieveDoc()", null)
        Boolean simple = request.getParameter('simple')?.equalsIgnoreCase('true')
        String jsonPath = request.getParameter('jsonpath')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.readOnlyCtxDtl(collection, customDetailJSON)
        ServletOutputStream outstream = response.outputStream
        Writer writer = new OutputStreamWriter(outstream)
        if (simple) {
            api.getSimpleDoc(ctxDtl.ctx, ctxDtl.dtl, uuid, writer)
        } else if (jsonPath != null) {
            api.getDocJsonPath(ctxDtl.ctx, ctxDtl.dtl, uuid, jsonPath, writer)
        } else {
            api.getDoc(ctxDtl.ctx, ctxDtl.dtl, uuid, writer)
        }
        writer.flush()
    }

    @RequestMapping(value = '/doc/{collection}/{id}/{attr}', method = RequestMethod.GET)
    void getAttr(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            @PathVariable(value = 'attr', required = true) String attr,
            //@RequestParam(value = 'simple', required = false) Boolean simple,
            //@RequestParam(value = 'jsonPath', required = false) String jsonPath = null,
            //@RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON,
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        log.inf("GET /doc/$collection/$uuid/$attr --> retrieveAttr()", null)
        Boolean simple = request.getParameter('simple')?.equalsIgnoreCase('true')
        String jsonPath = request.getParameter('jsonpath')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.readOnlyCtxDtl(collection, customDetailJSON)
        ServletOutputStream outstream = response.outputStream
        Writer writer = new OutputStreamWriter(outstream)
        if (simple) {
            api.getSimpleAttr(ctxDtl.ctx, ctxDtl.dtl, uuid, attr, writer)
        } else if (jsonPath != null) {
            api.getAttrJsonPath(ctxDtl.ctx, ctxDtl.dtl, uuid, attr, jsonPath, writer)
        } else {
            api.getAttr(ctxDtl.ctx, ctxDtl.dtl, uuid, attr, writer)
        }
        writer.flush()
    }

    @RequestMapping(value = ['/doc/{collection}', '/doc/{collection}/'], method = RequestMethod.POST)
    String newDoc(
            @PathVariable(value = 'collection', required = true) String collection,
            // CEM: request params aren't working (at least not in spring boot test), might be the httpreq injection
            //@RequestParam(value = 'async', required = false) Boolean async = false,
            //@RequestParamJSON(value = 'detail', required = false) String customDetailJSON,
            HttpServletRequest request
    ) {
        log.inf("POST /doc/$collection --> newDoc()", null)
        Boolean async = request.getParameter('async')?.equalsIgnoreCase('true')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.writeOnlyCtxDtl(collection, customDetailJSON)
        ServletInputStream instream = request.inputStream
        Reader reader = new InputStreamReader(instream)
        // TODO: figure out async use cases
        api.newDoc(ctxDtl.ctx, ctxDtl.dtl, reader)
    }

    @RequestMapping(value = ['/doc/{collection}/{id}/{attr}', '/doc/{collection}/{id}/{attr}/'], method = RequestMethod.POST)
    String newAttr(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            @PathVariable(value = 'attr', required = true) String attr,
            //@RequestParam(value = 'async', required = false) Boolean async = false,
            //@RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON,
            HttpServletRequest request
    ) {
        log.inf("POST /doc/$collection/$attr --> newAttr()", null)
        Boolean async = request.getParameter('async')?.equalsIgnoreCase('true')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.ctxAndDtl(collection, customDetailJSON)
        ServletInputStream instream = request.inputStream
        Reader reader = new InputStreamReader(instream)
        // TODO: figure out async use cases
        // TODO: paxos
        api.newAttr(ctxDtl.ctx, ctxDtl.dtl, uuid, attr, reader, false)
    }

    @RequestMapping(value = '/doc/{collection}/{id}/{attr}', method = RequestMethod.PUT)
    void updateAttr(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            @PathVariable(value = 'attr', required = true) String attr,
            //@RequestParam(value = 'async', required = false) Boolean async = false,
            //@RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON,
            HttpServletRequest request
    ) {
        log.inf("PUT /doc/$collection/$attr --> updateAttr()", null)
        Boolean async = request.getParameter('async')?.equalsIgnoreCase('true')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.ctxAndDtl(collection, customDetailJSON)
        ServletInputStream instream = request.inputStream
        Reader reader = new InputStreamReader(instream)
        String json = IOUtils.toString(reader)
        // TODO: figure out async use cases
        api.updateAttr(ctxDtl.ctx, ctxDtl.dtl, uuid, attr, json)
    }

    @RequestMapping(value = '/doc/{collection}/{id}/{attr}', method = RequestMethod.PATCH)
    void overlayAttr(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            @PathVariable(value = 'attr', required = true) String attr,
            //@RequestParam(value = 'async', required = false) Boolean async = false,
            //@RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON,
            HttpServletRequest request
    ) {
        log.inf("PATCH /doc/$collection/$attr --> overlayAttr()", null)
        Boolean async = request.getParameter('async')?.equalsIgnoreCase('true')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.ctxAndDtl(collection, customDetailJSON)
        ServletInputStream instream = request.inputStream
        Reader reader = new InputStreamReader(instream)
        String json = IOUtils.toString(reader)
        // TODO: figure out async use cases
        api.updateAttrOverlay(ctxDtl.ctx, ctxDtl.dtl, uuid, attr, json)
    }

    @RequestMapping(value = '/docs/{collection}', method = RequestMethod.POST)
    String newDocs(
            @PathVariable(value = 'collectmsondraion', required = true) String collection,
            //@RequestParam(value = 'async', required = false) Boolean async = false,
            //@RequestParamJSON(value = 'detail', required = false) Detail customDetailJSON,
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        log.inf("POST /docs/$collection --> newDocs()", null)
        Boolean async = request.getParameter('async')?.equalsIgnoreCase('true')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.writeOnlyCtxDtl(collection, customDetailJSON)

        ServletInputStream instream = request.inputStream
        Reader reader = new InputStreamReader(instream)

        ServletOutputStream outstream = response.outputStream
        Writer writer = new OutputStreamWriter(outstream)

        // TODO: figure out async use cases
        api.newDocList(ctxDtl.ctx, ctxDtl.dtl, reader, writer)

        writer.flush()
    }

    @RequestMapping(value = '/doc/{collection}/{id}', method = RequestMethod.DELETE)
    String delDoc(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        log.inf("DELETE /doc/$collection/$uuid --> delDoc()", null)
        Boolean retrieveDoomedDoc = request.getParameter('getBeforeDelete')?.equalsIgnoreCase('true')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.ctxAndDtl(collection, customDetailJSON)
        api.delDoc(ctxDtl.ctx, ctxDtl.dtl, uuid)
    }

    @RequestMapping(value = '/doc/{collection}/{id}/{attr}', method = RequestMethod.DELETE)
    String delAttr(
            @PathVariable(value = 'collection', required = true) String collection,
            @PathVariable(value = 'id', required = true) String uuid,
            @PathVariable(value = 'attr', required = true) String attr,
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        log.inf("DELETE /doc/$collection/$uuid/$attr --> delAttr()", null)
        Boolean retrieveDoomedAttr = request.getParameter('getBeforeDelete')?.equalsIgnoreCase('true')
        Detail customDetailJSON = (Detail) JSONUtil.deserialize(request.getParameter('detail'), Detail)

        CtxDtl ctxDtl = prepareCtx.ctxAndDtl(collection, customDetailJSON)
        api.delAttr(ctxDtl.ctx, ctxDtl.dtl, uuid, attr)
    }

}
