package _schema.cass

class CassSchemaGen {

  def entityTable(String keyspace, String entityTypeSuffix, List fixedCols) {
    StringBuilder sb = new StringBuilder()
    sb.append """
CREATE TABLE ${keyspace}.e_${entityTypeSuffix} (
  e text,
  a0 text,
  z_md text,
  zv uuid,
"""
    for (List fixedCol : fixedCols) {
      sb.append fixedCol[0]+" "+fixedCol[1]+",\n"
    }
    sb.append """
  PRIMARY KEY (e)
);
"""
    return sb.toString()
  }

  def attrTable(String keyspace, String entityTypeSuffix) {
    """
CREATE TABLE ${keyspace}.p_${entityTypeSuffix} (
  e text,
  p text,
  d text,
  t text,
  z_md text,
  zv uuid,
  PRIMARY KEY ((e),p)
);
"""
  }
}


