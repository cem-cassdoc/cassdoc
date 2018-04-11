package cassdoc.jcr

import javax.jcr.AccessDeniedException
import javax.jcr.Binary
import javax.jcr.InvalidItemStateException
import javax.jcr.Item
import javax.jcr.ItemExistsException
import javax.jcr.ItemNotFoundException
import javax.jcr.ItemVisitor
import javax.jcr.Node
import javax.jcr.Property
import javax.jcr.PropertyType
import javax.jcr.ReferentialIntegrityException
import javax.jcr.RepositoryException
import javax.jcr.Session
import javax.jcr.Value
import javax.jcr.ValueFormatException
import javax.jcr.lock.LockException
import javax.jcr.nodetype.ConstraintViolationException
import javax.jcr.nodetype.NoSuchNodeTypeException
import javax.jcr.nodetype.PropertyDefinition
import javax.jcr.version.VersionException

import cassdoc.CassDocJsonUtil
import cassdoc.DBCodes
import cassdoc.Detail
import cassdoc.OperationContext

class CassDocJcrProperty implements Property {
  String docId
  String propName
  CassDocJcrValue value
  transient CassDocJcrNode node
  transient CassDocJcrRepository repo

  boolean isNew = true
  boolean isModified = true
  boolean keepChanges = false


  @Override
  public Binary getBinary() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean getBoolean() throws ValueFormatException, RepositoryException {
    if (value instanceof boolean)
      return (boolean) value
    throw new ValueFormatException("CassDoc JCR property is not a decimal $docId $propName $value.typeCode")
  }

  @Override
  public Calendar getDate() throws ValueFormatException, RepositoryException {
    if (value instanceof BigInteger) {
      Date date = new Date(((BigInteger)value).longValue())
      Calendar cal = Calendar.getInstance()
      cal.setTime(date)
      return cal
    }
    throw new ValueFormatException("CassDoc JCR property does not support dates $docId $propName $value.typeCode")
  }

  @Override
  public BigDecimal getDecimal() throws ValueFormatException, RepositoryException {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value
    }
    throw new ValueFormatException("CassDoc JCR property is not a decimal $docId $propName $value.typeCode")
  }

  @Override
  public PropertyDefinition getDefinition() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double getDouble() throws ValueFormatException, RepositoryException {
    if (value instanceof BigDecimal) {
      return ((BigDecimal)value).doubleValue
    }
    throw new ValueFormatException("CassDoc JCR property is not a decimal $docId $propName $value.typeCode")
  }

  @Override
  public long getLength() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long[] getLengths() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getLong() throws ValueFormatException, RepositoryException {
    if (value.typeCode == DBCodes.TYPE_CODE_INTEGER) {
      return ((BigInteger)value).longValue()
    }
    throw new ValueFormatException("CassDoc JCR property is not a integer $docId $propName $value.typeCode")
  }

  @Override
  public Node getNode() throws ItemNotFoundException, ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public Property getProperty() throws ItemNotFoundException, ValueFormatException, RepositoryException {
    // gets another property if this property is a path... use Rels?
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getStream() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getString() throws ValueFormatException, RepositoryException {
    if (value.typeCode == DBCodes.TYPE_CODE_OBJECT) {
      StringWriter sw = new StringWriter()
      CassDocJsonUtil.specialSerialize(value,sw)
      return sw.toString()
    }
    if (value == null) return null;
    return value.toString();
  }

  /**
   * What a shitty set of property types, nice job JCR... no big decimals, big ints, lists, maps, sets etc?
   * 
   * WEAKREFERENCE will be used for arrays and child objects
   * 
   */
  @Override
  public int getType() throws RepositoryException {
    if (value.typeCode == DBCodes.TYPE_CODE_STRING) return PropertyType.STRING
    if (value.typeCode == DBCodes.TYPE_CODE_DECIMAL) return PropertyType.DOUBLE
    if (value.typeCode == DBCodes.TYPE_CODE_INTEGER) return PropertyType.LONG
    if (value.typeCode == DBCodes.TYPE_CODE_BOOLEAN) return PropertyType.BOOLEAN
    if (value.typeCode == DBCodes.TYPE_CODE_ARRAY) return PropertyType.WEAKREFERENCE
    if (value.typeCode == DBCodes.TYPE_CODE_OBJECT) return PropertyType.WEAKREFERENCE
    if (value.typeCode == null && value == null) return 0
    throw new RepositoryException("CassDoc JCR property has no type $docId $propName $value.typeCode")
  }

  @Override
  public Value getValue() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Value[] getValues() throws ValueFormatException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isMultiple() throws RepositoryException {
    // List/objects are considered "multiple". All else is not
    if (value.typeCode == DBCodes.TYPE_CODE_ARRAY || value.typeCode == DBCodes.TYPE_CODE_OBJECT) {
      return true
    }
    return false
  }

  @Override
  public void setValue(BigDecimal arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value = new CassDocJcrValue()
    value.typeCode = DBCodes.TYPE_CODE_DECIMAL
    value.value = arg0
    isModified = true
    node.propertyChanges[propName] = this
  }

  @Override
  public void setValue(Binary arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    throw new UnsupportedOperationException("TODO: blobs/clobs")
  }

  @Override
  public void setValue(boolean arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value = new CassDocJcrValue()
    value.typeCode = DBCodes.TYPE_CODE_BOOLEAN
    value.value = arg0
    isModified = true
    node.propertyChanges[propName] = this
  }

  @Override
  public void setValue(Calendar arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value = new CassDocJcrValue()
    value.typeCode = DBCodes.TYPE_CODE_INTEGER
    value.value = BigInteger.valueOf(arg0.getTimeInMillis())
    isModified = true
    node.propertyChanges[propName] = this
  }

  @Override
  public void setValue(double arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value = new CassDocJcrValue()
    value.typeCode = DBCodes.TYPE_CODE_DECIMAL
    value.value = new BigDecimal(arg0)
    isModified = true
    node.propertyChanges[propName] = this
  }

  @Override
  public void setValue(InputStream arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    throw new UnsupportedOperationException("TODO: blobs/clobs")
  }

  @Override
  public void setValue(long arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value = new CassDocJcrValue()
    value.typeCode = DBCodes.TYPE_CODE_INTEGER
    value.value = BigInteger.valueOf(arg0)
    isModified = true
    node.propertyChanges[propName] = this
  }

  /**
   * The setValue(Node) will be our means of doing complicated attribute operations such as:
   * 
   * - set the attribute/property to a JSON object (could be a simple JSON object or one with child documents)
   * - perform an attribute/property overlay
   * - add a relationship
   * - delete a relationship
   * 
   */
  @Override
  public void setValue(Node nodeValueType) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    if (nodeValueType instanceof CassDocJcrNewChildDoc) {
      node.propertyChanges[propName] = this

    }
    if (nodeValueType instanceof CassDocJcrOverlay) {
      node.propertyChanges[propName] = this

    }
    if (nodeValueType instanceof CassDocJcrAddRel) {
      node.propertyChanges[propName] = this

    }
    if (nodeValueType instanceof CassDocJcrDelRel) {
      node.propertyChanges[propName] = this

    }
    if (nodeValueType instanceof CassDocJcrObjectValue) {
      node.propertyChanges[propName] = this

    }
    if (nodeValueType instanceof CassDocJcrNode) {
      // what is the default value if a JCR Node is passed? ... must be a new child doc node with the -type _id?
    }

  }

  @Override
  public void setValue(String arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value = new CassDocJcrValue()
    value.typeCode = DBCodes.TYPE_CODE_STRING
    value.value = arg0
    isModified = true
    node.propertyChanges[propName] = this

  }

  @Override
  public void setValue(String[] arg0) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value.typeCode = DBCodes.TYPE_CODE_ARRAY
    value = Arrays.asList(arg0)
    isModified = true
    node.propertyChanges[propName] = this
  }

  @Override
  public void setValue(Value singleVal) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    CassDocJcrValue value = (CassDocJcrValue) arg0
    value = value.value
    isModified = true
    node.propertyChanges[propName] = this
  }

  @Override
  public void setValue(Value[] valList) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    value = new CassDocJcrValue()
    value.typeCode = DBCodes.TYPE_CODE_ARRAY
    List list = []
    for (Value v : values) {
      CassDocJcrValue val = (CassDocJcrValue)v
      list.add(val.value)
    }
    value.value = list
    isModified = true
    node.propertyChanges[propName] = this
  }

  @Override
  public void accept(ItemVisitor arg0) throws RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public Item getAncestor(int arg0) throws ItemNotFoundException, AccessDeniedException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getDepth() throws RepositoryException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getName() throws RepositoryException {
    return propName
  }

  @Override
  public Node getParent() throws ItemNotFoundException, AccessDeniedException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPath() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Session getSession() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isModified() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isNew() {
    isNew
  }

  @Override
  public boolean isNode() {
    false
  }

  @Override
  public boolean isSame(Item arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void refresh(boolean arg0) throws InvalidItemStateException, RepositoryException {

    // does this re-pull from the database


    // TODO Auto-generated method stub

  }

  @Override
  public void remove() throws VersionException, LockException, ConstraintViolationException, AccessDeniedException, RepositoryException {
    repo.cassDocAPI.delAttr(new OperationContext(space:repo.space), new Detail(), docId, propName)
  }

  @Override
  public void save() throws AccessDeniedException, ItemExistsException, ConstraintViolationException, InvalidItemStateException, ReferentialIntegrityException, VersionException, LockException, NoSuchNodeTypeException, RepositoryException {
    // the
    if (isNew) {
      repo.cassDocAPI.updateAttrEntry(new OperationContext(space:repo.space), new Detail(), docId, new AbstractMap.SimpleEntry<String,Object>(key:propName,value:value))
    }


  }

  @Override
  public Object getProperty(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }
}
