package cassdoc.springmvc.service

import cassdoc.Detail
import cassdoc.OperationContext
import groovy.transform.CompileStatic
import org.springframework.stereotype.Component

@CompileStatic
@Component
class PrepareCtx {

    CtxDtl readOnlyCtxDtl(String collection, Detail customDetail) {
        if (customDetail != null) {
            if (customDetail.writeConsistency != null) {
                throw new IllegalArgumentException('writeConsistency set for a read-only API')
            }
            if (customDetail.writeTimestampMicros != null) {
                throw new IllegalArgumentException('writeTimestampMicros set for a read-only API')
            }
        }
        ctxAndDtl(collection, customDetail)
    }

    CtxDtl writeOnlyCtxDtl(String collection, Detail customDetail) {
        if (customDetail != null) {
            if (customDetail.readConsistency != null) {
                throw new IllegalArgumentException('readConsistency set for a write-only API')
            }
        }
        ctxAndDtl(collection, customDetail)
    }

    CtxDtl ctxAndDtl(String collection, Detail customDetail) {
        OperationContext ctx = new OperationContext(space: collection)
        new CtxDtl(ctx: ctx, dtl: customDetail == null ? new Detail() : customDetail)
    }
}

@CompileStatic
class CtxDtl {
    OperationContext ctx
    Detail dtl
}