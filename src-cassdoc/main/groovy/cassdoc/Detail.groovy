package cassdoc

import com.fasterxml.jackson.annotation.JsonInclude
import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringUtils


@CompileStatic
@JsonInclude(JsonInclude.Include.NON_NULL)
class Detail {
    String readConsistency = null
    String writeConsistency = null
    Long writeTimestampMicros = null
    Long timeoutMillis = null
    // TODO: deletion cascade modes
    // TODO: attr name regex filters


    Set<String> attrSubset = null
    Set<String> attrExclude = null
    Map<String, Detail> attrDetail = null

    Boolean pullChildDocs  = null
    Detail childDocDetail = null
    Map<String, Detail> childDocSuffixDetail = null

    // meta attributes
    Boolean docIDTimestampMeta  = null // y
    Boolean docIDDateMeta  = null // y
    String docWritetimeMeta = null // y
    String attrWritetimeMeta = null // y
    Boolean docWritetimeDateMeta  = null // y
    Boolean attrWritetimeDateMeta  = null //y
    Boolean docPaxosMeta  = null  // y
    Boolean attrPaxosMeta  = null // y
    Boolean docPaxosTimestampMeta  = null  // y
    Boolean attrPaxosTimestampMeta  = null // y
    Boolean docPaxosDateMeta  = null  // y
    Boolean attrPaxosDateMeta  = null // y
    Boolean docMetaIDMeta  = null // y
    Boolean docMetaDataMeta  = null // y
    Boolean attrMetaIDMeta  = null  // y
    Boolean attrMetaDataMeta  = null // y
    //Boolean relMetaIDMeta  = null // TODO
    //Boolean relMetaDataMeta  = null // TODO
    Boolean parentMeta  = null // y
    Boolean docChildrenMeta  = null // y
    Boolean docRelationsMeta  = null // y
    Boolean docTokenMeta  = null // y
    Boolean attrTokenMeta  = null // y

    // TODO: relation retrieval settings

    String searchStartToken = null
    String searchStopToken = null
    String searchEntityLimit = null
    Integer fetchNextPageThreshold = null
    Integer fetchPageSize = null

    // batch vs async spray vs as-you-go specifiers...

    // Async vs Streaming vs "DOM" is API signature specific

    Detail resolveAttrDetail(String attr) {
        if (attrSubset != null) {
            if (!attrSubset.contains(attr)) {
                return null
            }
        }
        if (attrExclude != null) {
            if (attrExclude.contains(attr)) {
                return null
            }
        }
        if (attrDetail == null) {
            return this
        }
        Detail attrDtl = attrDetail[attr]
        if (attrDtl == null) {
            return this
        }
        return attrDtl
    }

    Detail resolveChildDocDetail(String childDocUUID, String attr) {
        // no attr detail necessary, since we have already done so in the attribute iteration
        if (pullChildDocs) {
            if (childDocSuffixDetail != null) {
                String suffix = IDUtil.idSuffix(childDocUUID)
                Detail suffixDetail = childDocSuffixDetail?.get(suffix)
                if (suffixDetail != null) {
                    return suffixDetail
                }
            }
            if (childDocDetail != null) {
                return childDocDetail
            } else {
                // no detail provided, but we are exhorted to pull child docs, so we'll return the last detail provided
                return this
            }

        } else {
            // suffix overrides can override pullChildDocs global flag
            if (childDocSuffixDetail != null) {
                String suffix = IDUtil.idSuffix(childDocUUID)
                Detail suffixDetail = childDocSuffixDetail?.get(suffix)
                if (suffixDetail != null) {
                    return suffixDetail
                }
            }

        }
        // return null to indicate do NOT pull child docs
        return null
    }

    String resolveReadConsistency(Detail detail, OperationContext opctx) {
        StringUtils.isEmpty(detail?.readConsistency) ? opctx.readConsistency : detail.readConsistency
    }

    String resolveWriteConsistency(Detail detail, OperationContext opctx) {
        StringUtils.isEmpty(detail?.writeConsistency) ? opctx.writeConsistency : detail.writeConsistency
    }

}
