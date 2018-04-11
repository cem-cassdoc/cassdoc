package cassdoc

class CassandraSchemaUtil {

    static String createSchemaKeyspace(int replicationFactor = 1) {
        """
        | CREATE KEYSPACE IF NOT EXISTS cassdoc_system_schema WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '$replicationFactor'}  AND durable_writes = true;
        """.stripMargin()
    }

    static String createSchemaTypesTable() {
        """
        | CREATE TABLE cassdoc_system_schema.types (
        |   ks text,
        |   nm text,
        |   json text,
        |   PRIMARY KEY ((ks),nm)
        | );
        """.stripMargin()
    }

    static String createSchemaIndexesTable() {
        """
        | CREATE TABLE cassdoc_system_schema.indexes (
        |   ks text,
        |   nm text,
        |   json text,
        |   PRIMARY KEY ((ks),nm)
        | );
        """.stripMargin()
    }

    static String dropKeyspace(String keyspace) {
        """
        | DROP KEYSPACE IF EXISTS ${keyspace};
        """.stripMargin()
    }

    static String createKeyspace(String keyspace, int replicationFactor = 1) {
        """
        | CREATE KEYSPACE $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '$replicationFactor'}  AND durable_writes = true;
        """.stripMargin()
    }

    static String createEntityTable(String keyspace, String entityTypeSuffix, List<FixedAttr> fixedCols) {
        StringBuilder sb = new StringBuilder()
        sb.append("""
        | CREATE TABLE ${keyspace}.e_${entityTypeSuffix} (
        |   e text,
        |   a0 text,
        |   z_md text,
        |   zv uuid,
        """.stripMargin())

        fixedCols.each { fixedCol -> sb.append "${fixedCol.colname} ${fixedCol.coltype},\n" }

        sb.append("""
        |   PRIMARY KEY (e)
        | );
        """.stripMargin())
        return sb.toString()
    }

    static String createAttrTable(String keyspace, String entityTypeSuffix) {
        """
        | CREATE TABLE ${keyspace}.p_${entityTypeSuffix} (
        |   e text,
        |   p text,
        |   d text,
        |   t text,
        |   z_md text,
        |   zv uuid,
        |   PRIMARY KEY ((e),p)
        | );
        """.stripMargin()
    }

    static String createRelationTable(String keyspace) {
        """
        | CREATE TABLE ${keyspace}.r (
        |   p1 text,
        |   ty1 text,
        |   ty2 text,
        |   ty3 text,
        |   ty4 text,
        |   p2 text,
        |   p3 text,
        |   p4 text,
        |   c1 text,
        |   c2 text,
        |   c3 text,
        |   c4 text,
        |   d text,
        |   link text,
        |   z_md text,
        |   PRIMARY KEY ((p1),ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4)
        | );
        """.stripMargin()
    }

    static String createIndexTable(String keyspace) {
        """
        | CREATE TABLE ${keyspace}.i (
        |   i1 text,
        |   i2 text,
        |   i3 text,
        |   k1 text,
        |   k2 text,
        |   k3 text,
        |   v1 text,
        |   v2 text,
        |   v3 text,
        |   id text,
        |   d text,
        |   PRIMARY KEY ((i1,i2,i3,k1,k2,k3),v1,v2,v3)
        | );
        """.stripMargin()
    }

    static String insertSchemaType() {
        "INSERT INTO cassdoc_system_schema.types (ks, nm, json) VALUES (?, ?, ?);"
    }

}
