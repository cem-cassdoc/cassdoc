package cassdoc

class DBCodes {
    public static final TYPE_CODE_STRING = 'S'
    public static final TYPE_CODE_DECIMAL = 'D'
    public static final TYPE_CODE_INTEGER = 'I'
    public static final TYPE_CODE_OBJECT = 'O'
    public static final TYPE_CODE_ARRAY = 'A'
    public static final TYPE_CODE_BOOLEAN = 'B'
}

class RelTypes {
    public static final String SYS_INDEX = '_I'
    public static final String TO_CHILD = 'CH'
    public static final String TO_PARENT = '-CH'
}

class AttrNames {
    public static final String SYS_DOCID = '_id'
    public static final String SYS_BLOBID = '_blob'
    public static final String SYS_CLOBID = '_clob'

    public static final String META_TOKEN = '@[TOKEN]'
    public static final String META_IDTIME = '@[IDTIME]'
    public static final String META_IDDATE = '@[IDDATE]'
    public static final String META_PAXOS = '@[PAXOS]'
    public static final String META_PAXOSTIME = '@[PAXOSTIME]'
    public static final String META_PAXOSDATE = '@[PAXOSDATE]'
    public static final String META_DOCMETAID = '@[DOCMETAID]'
    public static final String META_DOCMETADATA = '@[DOCMETADATA]'
    public static final String META_PARENT = '@[PARENT]'
    public static final String META_WT_PRE = '@[WT_'
    public static final String META_WTDT_PRE = '@[WTDT_'
    public static final String META_RELS = '@[RELS]'
    public static final String META_CHILDREN = '@[CHILDREN]'
    public static final String META_ATTRMETAID = '@[ATTRMETAID]'
    public static final String META_ATTRMETADATA = '@[ATTRMETADATA]'
    public static final String META_CQLTRACE = '@[CQLTRACE]'
}

class IndexTypes {
    // simple single-attribute HAS-VALUE
    public static final String HAS_VALUE = 'HAS_VALUE'
    public static final String HAS_VALUE_CODE = 'HV'

    // not implemented yet...
    public static final String MOST_RECENT = 'MOST_RECENT'
    public static final String MOST_RECENT_CODE = 'MR'

}