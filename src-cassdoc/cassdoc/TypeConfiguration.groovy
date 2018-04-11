package cassdoc

import groovy.transform.CompileStatic

import javax.naming.ConfigurationException

import cwdrg.lg.annotation.Log

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
  transient DocType docType
}

@CompileStatic
class DocType {
  String uri
  String suffix
  Map<String,FixedAttr> fixedAttrMap = [:]
  Map<String,ManualIndex> indexMap = [:]
  Map<String,Set<ManualIndex>> attrIndexMap = [:]
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
        SetMap.put(attrIndexMap,attr,idx)
      }
    }
  }
}


// NULL problem: null == '' for now
// indexing non-strings: 0 padding? <-- that will truly require a special index

@CompileStatic
class ManualIndex {
  String indexRef  // identifier (must be unique for doctype or bad things happen on cleanup)
  String indexType  // low-card hash lookup, hashbucket b+ for LIKE support, time buckets, history (zero-pad the time by a couple 0s), tags, functional
  Map indexConfig   // index-type specific configuration infomation
  List<String> indexCodes = [] // i1,i2,i3
  List<String> keyAttrs = [] // attributes that form the k1 / k2 / k3 fields
  transient DocType docType
}

@CompileStatic
@Log
class TypeConfigurationService {

  Map<String, DocType> suffixTypeMap = [:]
  Map<String, DocType> uriTypeMap = [:]

  List<DocType> getTypeList() {
    uriTypeMap.values().asList()
  }
  void setTypeList(List<DocType> typeList) {
    uriTypeMap.clear()
    suffixTypeMap.clear()
    for (DocType type : typeList) {
      if (uriTypeMap.containsKey(type.uri)) {throw log.err("", new ConfigurationException("Duplicate DocType uri: ${type.uri}"))}
      if (suffixTypeMap.containsKey(type.suffix)) {throw log.err("", new ConfigurationException("Duplicate DocType suffix: ${type.suffix}"))}
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

  static String attrTypeCode(Class type)
  {
    if (type == String.class) {return DBCodes.TYPE_CODE_STRING}
    if (type == null) { return null }
    if (type == List.class) {return DBCodes.TYPE_CODE_ARRAY}
    if (type == Map.class) {return DBCodes.TYPE_CODE_OBJECT}
    if (type == BigInteger.class) {return DBCodes.TYPE_CODE_INTEGER}
    if (type == Integer.class) {return DBCodes.TYPE_CODE_INTEGER}
    if (type == BigDecimal.class) {return DBCodes.TYPE_CODE_DECIMAL}
    if (type == Double.class) {return DBCodes.TYPE_CODE_DECIMAL}
    if (type == Float.class) {return DBCodes.TYPE_CODE_DECIMAL}
    if (type == Boolean.class) {return DBCodes.TYPE_CODE_BOOLEAN}
    throw new Exception("Unknown JSON type "+type.name)
  }

  static Class attrClass(String typeCode)
  {
    if (typeCode == DBCodes.TYPE_CODE_STRING) {return String.class}
    if (typeCode == null) { return null }
    if (typeCode == DBCodes.TYPE_CODE_ARRAY) {return List.class}
    if (typeCode == DBCodes.TYPE_CODE_OBJECT) {return Map.class}
    if (typeCode == DBCodes.TYPE_CODE_INTEGER) {return BigInteger.class}
    if (typeCode == DBCodes.TYPE_CODE_DECIMAL) {return BigDecimal.class}
    if (typeCode == DBCodes.TYPE_CODE_BOOLEAN) {return Boolean.class}
    throw new Exception("Unknown JSON typecode"+typeCode)
  }

}
