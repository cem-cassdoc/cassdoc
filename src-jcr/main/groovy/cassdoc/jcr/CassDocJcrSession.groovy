package cassdoc.jcr

import java.security.AccessControlException

import javax.jcr.AccessDeniedException
import javax.jcr.Credentials
import javax.jcr.InvalidItemStateException
import javax.jcr.InvalidSerializedDataException
import javax.jcr.Item
import javax.jcr.ItemExistsException
import javax.jcr.ItemNotFoundException
import javax.jcr.LoginException
import javax.jcr.NamespaceException
import javax.jcr.Node
import javax.jcr.PathNotFoundException
import javax.jcr.Property
import javax.jcr.ReferentialIntegrityException
import javax.jcr.Repository
import javax.jcr.RepositoryException
import javax.jcr.Session
import javax.jcr.UnsupportedRepositoryOperationException
import javax.jcr.ValueFactory
import javax.jcr.Workspace
import javax.jcr.lock.LockException
import javax.jcr.nodetype.ConstraintViolationException
import javax.jcr.nodetype.NoSuchNodeTypeException
import javax.jcr.retention.RetentionManager
import javax.jcr.security.AccessControlManager
import javax.jcr.version.VersionException

import org.xml.sax.ContentHandler
import org.xml.sax.SAXException

class CassDocJcrSession implements Session {

  CassDocJcrRepository repo;

  @Override
  public Property getProperty(String absolutePath) {
  }

  @Override
  public Object getAttribute(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getAttributeNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Item getItem(String absPath) throws PathNotFoundException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Node getNode(String arg0) throws PathNotFoundException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Node getNodeByIdentifier(String docId) throws ItemNotFoundException, RepositoryException {
    repo.cassDocAPI.deseria
  }

  @Override
  public Node getNodeByUUID(String docId) throws ItemNotFoundException, RepositoryException {
    getNodeByIdentifier(docId)
  }

  @Override
  public String getNamespacePrefix(String arg0) throws NamespaceException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getNamespacePrefixes() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNamespaceURI(String arg0) throws NamespaceException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Repository getRepository() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RetentionManager getRetentionManager() throws UnsupportedRepositoryOperationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Node getRootNode() throws RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getUserID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ValueFactory getValueFactory() throws UnsupportedRepositoryOperationException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Workspace getWorkspace() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasCapability(String arg0, Object arg1, Object[] arg2) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean hasPendingChanges() throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean hasPermission(String arg0, String arg1) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Session impersonate(Credentials arg0) throws LoginException, RepositoryException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public boolean isLive() {
    return repo != null;
  }

  @Override
  public boolean itemExists(String absPath) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void logout() {
    // TODO Auto-generated method stub

  }

  @Override
  public void move(String srcAbsPath, String destAbsPath) throws ItemExistsException, PathNotFoundException, VersionException, ConstraintViolationException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public boolean nodeExists(String absolutePath) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean propertyExists(String absolutePath) throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void refresh(boolean arg0) throws RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeItem(String arg0) throws VersionException, LockException, ConstraintViolationException, AccessDeniedException, RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeLockToken(String arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void save() throws AccessDeniedException, ItemExistsException, ReferentialIntegrityException, ConstraintViolationException, InvalidItemStateException, VersionException, LockException, NoSuchNodeTypeException, RepositoryException {
    throw new UnsupportedOperationException("save should be performed at the document level")
  }

  @Override
  public void setNamespacePrefix(String arg0, String arg1) throws NamespaceException, RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void addLockToken(String lt) {
    throw new UnsupportedOperationException()
  }

  @Override
  public void checkPermission(String arg0, String arg1) throws AccessControlException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public ContentHandler getImportContentHandler(String arg0, int arg1) throws PathNotFoundException, ConstraintViolationException, VersionException, LockException, RepositoryException {
    throw new UnsupportedOperationException("XML sucks")
  }

  @Override
  public void exportDocumentView(String absPath, ContentHandler arg1, boolean skipBinary, boolean noRecurse) throws PathNotFoundException, SAXException, RepositoryException {
    // XML sucks
    throw new UnsupportedOperationException("XML sucks")
  }

  @Override
  public void exportSystemView(String absPath, ContentHandler arg1, boolean skipBinary, boolean noRecurse) throws PathNotFoundException, SAXException, RepositoryException {
    // XML sucks
    throw new UnsupportedOperationException("XML sucks")
  }

  @Override
  public void exportDocumentView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse) throws IOException, PathNotFoundException, RepositoryException {
    throw new UnsupportedOperationException("XML sucks")
  }

  @Override
  public void exportSystemView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse) throws IOException, PathNotFoundException, RepositoryException {
    throw new UnsupportedOperationException("XML sucks")
  }

  @Override
  public AccessControlManager getAccessControlManager() throws UnsupportedRepositoryOperationException, RepositoryException {
    throw new UnsupportedOperationException()
  }

  @Override
  public String[] getLockTokens() {
    throw new UnsupportedOperationException("XML sucks")
  }

  @Override
  public void importXML(String arg0, InputStream arg1, int arg2) throws IOException, PathNotFoundException, ItemExistsException, ConstraintViolationException, VersionException, InvalidSerializedDataException, LockException, RepositoryException {
    throw new UnsupportedOperationException()
  }
}
