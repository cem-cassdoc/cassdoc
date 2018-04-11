package cassdoc

import com.fasterxml.jackson.annotation.JsonIgnore
import cwdrg.lg.annotation.Log
import groovy.transform.CompileStatic

import javax.naming.ConfigurationException

@CompileStatic
interface ValueExtractor {
    Object extractValue(String json)
}

@CompileStatic
class FixedAttr {
    String sourceAttr
    String colname
    String coltype
    Object jsonExtractor
    String prepStmt_UPDATE_ATTR
    String prepStmt_UPDATE_ATTR_PAXOS
    @JsonIgnore DocType docType
}

@CompileStatic
class DocType {
    String uri
    String suffix
    Map<String, FixedAttr> fixedAttrMap = [:]
    Map<String, ManualIndex> indexMap = [:]
    Map<String, Set<ManualIndex>> attrIndexMap = [:]
    Object aliasPolicy
    Object generalAttributePolicy

    // virtual init attr for fixed Attrs
    List<FixedAttr> getFixedAttrList() {
        fixedAttrMap.values().asList()
    }

    void setFixedAttrList(List<FixedAttr> fixedAttrList) {
        for (FixedAttr fixedAttr : fixedAttrList) {
            fixedAttrMap[fixedAttr.sourceAttr] = fixedAttr
            fixedAttr.docType = this
        }
    }

    List<ManualIndex> getManualIndexList() {
        indexMap.values().asList()
    }

    void setManualIndexList(List<ManualIndex> manualIndexList) {
        for (ManualIndex idx : manualIndexList) {
            idx.docType = this
            indexMap[idx.indexRef] = idx
            for (String attr : idx.keyAttrs) {
                SetMap.put(attrIndexMap, attr, idx)
            }
        }
    }
}

// NULL problem: null == '' for now
// indexing non-strings: 0 padding? <-- that will truly require a special index

@CompileStatic
class ManualIndex {
    String indexRef  // identifier (must be unique for doctype or bad things happen on cleanup)
    String indexType
    // low-card hash lookup, hashbucket b+ for LIKE support, time buckets, history (zero-pad the time by a couple 0s), tags, functional
    Map indexConfig   // index-type specific configuration infomation
    List<String> indexCodes = [] // i1,i2,i3
    List<String> keyAttrs = [] // attributes that form the k1 / k2 / k3 fields
    @JsonIgnore DocType docType
}

@CompileStatic
@Log
class TypeConfigurationService {

    final Map<String, DocType> suffixTypeMap = [:]
    final Map<String, DocType> uriTypeMap = [:]

    List<DocType> getTypeList() {
        uriTypeMap.values().asList()
    }

    void setTypeList(List<DocType> typeList) {
        uriTypeMap.clear()
        suffixTypeMap.clear()
        for (DocType type : typeList) {
            if (uriTypeMap.containsKey(type.uri)) {
                throw log.err("", new ConfigurationException("Duplicate DocType uri: ${type.uri}"))
            }
            if (suffixTypeMap.containsKey(type.suffix)) {
                throw log.err("", new ConfigurationException("Duplicate DocType suffix: ${type.suffix}"))
            }
            uriTypeMap[type.uri] = type
            suffixTypeMap[type.suffix] = type
        }
    }

    DocType getTypeForSuffix(String suffix) {
        suffixTypeMap[suffix]
    }

    String getSuffixForType(String typeURI) {
        uriTypeMap[typeURI]?.suffix
    }

    DocType getTypeForID(String entityUUID) {
        getTypeForSuffix(IDUtil.idSuffix(entityUUID))
    }

    boolean isKnownSuffix(String entityUUIDSuffix) {
        suffixTypeMap.containsKey(entityUUIDSuffix)
    }

    // ---- utils

    static String attrTypeCode(Class type) {
        if (type == String) {
            return "S"
        }
        if (type == null) {
            return null
        }
        if (type == List) {
            return "A"
        }
        if (type == Map) {
            return "O"
        }
        if (type == Integer) {
            return "I"
        }
        if (type == Float) {
            return "D"
        }
        if (type == Boolean) {
            return "B"
        }
        throw new IllegalArgumentException("Unknown JSON type " + type.name)
    }

    static Class attrClass(String typeCode) {
        if (typeCode == "S") {
            return String
        }
        if (typeCode == null) {
            return null
        }
        if (typeCode == "A") {
            return List
        }
        if (typeCode == "O") {
            return Map
        }
        if (typeCode == "I") {
            return Integer
        }
        if (typeCode == "D") {
            return Float
        }
        if (typeCode == "B") {
            return Boolean
        }
        throw new IllegalArgumentException("Unknown JSON typecode" + typeCode)
    }

}
