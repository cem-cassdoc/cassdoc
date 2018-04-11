package cassdoc.jcr

import javax.jcr.AccessDeniedException
import javax.jcr.Binary
import javax.jcr.InvalidItemStateException
import javax.jcr.InvalidLifecycleTransitionException
import javax.jcr.Item
import javax.jcr.ItemExistsException
import javax.jcr.ItemNotFoundException
import javax.jcr.ItemVisitor
import javax.jcr.MergeException
import javax.jcr.NoSuchWorkspaceException
import javax.jcr.Node
import javax.jcr.NodeIterator
import javax.jcr.PathNotFoundException
import javax.jcr.Property
import javax.jcr.PropertyIterator
import javax.jcr.ReferentialIntegrityException
import javax.jcr.RepositoryException
import javax.jcr.Session
import javax.jcr.UnsupportedRepositoryOperationException
import javax.jcr.Value
import javax.jcr.ValueFormatException
import javax.jcr.lock.Lock
import javax.jcr.lock.LockException
import javax.jcr.nodetype.ConstraintViolationException
import javax.jcr.nodetype.NoSuchNodeTypeException
import javax.jcr.nodetype.NodeDefinition
import javax.jcr.nodetype.NodeType
import javax.jcr.version.ActivityViolationException
import javax.jcr.version.Version
import javax.jcr.version.VersionException
import javax.jcr.version.VersionHistory

import cassdoc.Detail
import cassdoc.OperationContext

/**
 * All we are implementing is basic property and relationship interactions for now. 
 * 
 * No versioning, typing / schema enforcement, life cycling, locking, or merging operations are supported
 * 
 * @author cowardlydragon
 *
 */
class CassDocJcrNode implements Node {

  boolean isNew
  boolean isModified
  String docId

  Map<String,CassDocJcrProperty> propertyChanges = [:]

  transient CassDocJcrSession session
  transient CassDocJcrRepository repo

  @Override
  public Property getProperty(String propName) {
    CassDocJcrProperty p = new CassDocJcrProperty()
    CassDocJcrValue v = new CassDocJcrValue()
    v.value = repo.cassDocAPI.deserializeSimpleAttr(new OperationContext(space:repo.space), new Detail(), docId, propName)
    p.value = v
  }

  @Override
  public String getIdentifier() throws RepositoryException {
    docId
  }

  @Override
  public String getName() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Node getNode(String relativePath) throws PathNotFoundException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NodeIterator getNodes() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NodeIterator getNodes(String arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NodeIterator getNodes(String[] arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyIterator getProperties() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyIterator getProperties(String arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyIterator getProperties(String[] arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * get relations (references, etc)
   * 
   */
  @Override
  public PropertyIterator getReferences() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyIterator getReferences(String arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NodeIterator getSharedSet() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getUUID() throws UnsupportedRepositoryOperationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VersionHistory getVersionHistory() throws UnsupportedRepositoryOperationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  // weak refs are not CH.. no cascading? is that a good standard?
  @Override
  public PropertyIterator getWeakReferences() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyIterator getWeakReferences(String arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasNode(String arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean hasNodes() throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean hasProperties() throws RepositoryException {
    // it CAN so return true
    true
  }

  @Override
  public boolean hasProperty(String propName) throws RepositoryException {
    boolean resp =  repo.cassDocAPI.attrExists(new OperationContext(space:repo.space), new Detail(), docId, propName);
  }

  @Override
  public Property setProperty(String arg0, BigDecimal arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, Binary arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, boolean arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, Calendar arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, double arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, InputStream arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, long arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, Node arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, String arg1, int arg2) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, String arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, String[] arg1, int arg2) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, String[] arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, Value arg1, int arg2) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    isModified = true
    null
  }

  @Override
  public Property setProperty(String arg0, Value arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    isModified = true

    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, Value[] arg1, int arg2) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Property setProperty(String arg0, Value[] arg1) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void update(String arg0) throws NoSuchWorkspaceException, AccessDeniedException, LockException, InvalidItemStateException, RepositoryException {
    // TODO Auto-generated method stub

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
    throw new UnsupportedOperationException()
  }


  @Override
  public Node getParent() throws ItemNotFoundException, AccessDeniedException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPath() throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public Session getSession() throws RepositoryException {
    session
  }

  @Override
  public boolean isModified() {
    isModified
  }

  @Override
  public boolean isNew() {
    isNew
  }

  @Override
  public boolean isNode() {
    true
  }

  @Override
  public boolean isSame(Item arg0) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void refresh(boolean arg0) throws InvalidItemStateException, RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void remove() throws VersionException, LockException, ConstraintViolationException, AccessDeniedException, RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void save() throws AccessDeniedException, ItemExistsException, ConstraintViolationException, InvalidItemStateException, ReferentialIntegrityException, VersionException, LockException, NoSuchNodeTypeException, RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setProperty(String arg0, Object arg1) {
    // TODO Auto-generated method stub
  }


  @Override
  public void orderBefore(String arg0, String arg1) throws UnsupportedRepositoryOperationException, VersionException, ConstraintViolationException, ItemNotFoundException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public int getIndex() throws RepositoryException {
    throw new UnsupportedOperationException()
  }


  @Override
  public String getCorrespondingNodePath(String arg0) throws ItemNotFoundException, NoSuchWorkspaceException, AccessDeniedException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  /**
   * use for relationship creation? child doc creation? encode relationship type in relPath like a url?
   *
   */
  @Override
  public Node addNode(String relPath, String primaryNodeTypeName) throws ItemExistsException, PathNotFoundException, NoSuchNodeTypeException, LockException, VersionException, ConstraintViolationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public Node addNode(String relPath) throws ItemExistsException, PathNotFoundException, VersionException, ConstraintViolationException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public Item getPrimaryItem() throws ItemNotFoundException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void restore(String path, boolean removeExisting) throws VersionException, ItemExistsException, UnsupportedRepositoryOperationException, LockException, InvalidItemStateException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void restore(Version ver, boolean removeExisting) throws VersionException, ItemExistsException, InvalidItemStateException, UnsupportedRepositoryOperationException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void restore(Version ver, String verName, boolean removeExisting) throws PathNotFoundException, ItemExistsException, VersionException, ConstraintViolationException, UnsupportedRepositoryOperationException, LockException, InvalidItemStateException,
  RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void restoreByLabel(String arg0, boolean arg1) throws VersionException, ItemExistsException, UnsupportedRepositoryOperationException, LockException, InvalidItemStateException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public boolean isNodeType(String arg0) throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public NodeDefinition getDefinition() throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public NodeType getPrimaryNodeType() throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void setPrimaryType(String arg0) throws NoSuchNodeTypeException, VersionException, ConstraintViolationException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public NodeType[] getMixinNodeTypes() throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public boolean canAddMixin(String arg0) throws NoSuchNodeTypeException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void addMixin(String arg0) throws NoSuchNodeTypeException, VersionException, ConstraintViolationException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void removeMixin(String arg0) throws NoSuchNodeTypeException, VersionException, ConstraintViolationException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void removeShare() throws VersionException, LockException, ConstraintViolationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void removeSharedSet() throws VersionException, LockException, ConstraintViolationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public NodeIterator merge(String arg0, boolean arg1) throws NoSuchWorkspaceException, AccessDeniedException, MergeException, LockException, InvalidItemStateException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void doneMerge(Version arg0) throws VersionException, InvalidItemStateException, UnsupportedRepositoryOperationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void cancelMerge(Version arg0) throws VersionException, InvalidItemStateException, UnsupportedRepositoryOperationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void followLifecycleTransition(String arg0) throws UnsupportedRepositoryOperationException, InvalidLifecycleTransitionException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public String[] getAllowedLifecycleTransistions() throws UnsupportedRepositoryOperationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public boolean isCheckedOut() throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public Version checkin() throws VersionException, UnsupportedRepositoryOperationException, InvalidItemStateException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void checkout() throws UnsupportedRepositoryOperationException, LockException, ActivityViolationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public Version getBaseVersion() throws UnsupportedRepositoryOperationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public boolean holdsLock() throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public boolean isLocked() throws RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public Lock getLock() throws UnsupportedRepositoryOperationException, LockException, AccessDeniedException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public Lock lock(boolean arg0, boolean arg1) throws UnsupportedRepositoryOperationException, LockException, AccessDeniedException, InvalidItemStateException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public void unlock() throws UnsupportedRepositoryOperationException, LockException, AccessDeniedException, InvalidItemStateException, RepositoryException {
    throw new UnsupportedOperationException()
  }
}
