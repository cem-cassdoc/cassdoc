package cassdoc.jcr

import javax.jcr.Binary
import javax.jcr.Property
import javax.jcr.RepositoryException
import javax.jcr.Value
import javax.jcr.ValueFormatException

import cassdoc.DBCodes

class CassDocJcrValue implements Value {

  Object value

  transient Property prop
  transient CassDocJcrRepository


  public String getTypeCode() {
    if (value == null) return null
    if (value instanceof CharSequence) return DBCodes.TYPE_CODE_STRING
    if (value instanceof Map) return DBCodes.TYPE_CODE_OBJECT
    if (value instanceof List) return DBCodes.TYPE_CODE_ARRAY
    if (value instanceof BigInteger) return DBCodes.TYPE_CODE_INTEGER
    if (value instanceof BigDecimal) return DBCodes.TYPE_CODE_DECIMAL
    if (value instanceof Boolean) return DBCodes.TYPE_CODE_BOOLEAN
    return null
  }


  @Override
  public Binary getBinary() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean getBoolean() throws ValueFormatException, RepositoryException {
    if (typeCode == DBCodes.TYPE_CODE_BOOLEAN) {
      return (Boolean) value
    }
  }

  @Override
  public Calendar getDate() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BigDecimal getDecimal() throws ValueFormatException, RepositoryException {
    if (jsonType == 'D')
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public double getDouble() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getLong() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public InputStream getStream() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getString() throws ValueFormatException, IllegalStateException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getType() {
    // TODO Auto-generated method stub
    return 0;
  }
}
