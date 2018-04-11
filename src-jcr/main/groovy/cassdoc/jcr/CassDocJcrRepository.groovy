package cassdoc.jcr

import javax.jcr.Credentials
import javax.jcr.LoginException
import javax.jcr.NoSuchWorkspaceException
import javax.jcr.Repository
import javax.jcr.RepositoryException
import javax.jcr.Session
import javax.jcr.Value

import cassdoc.API

class CassDocJcrRepository implements Repository {

  String space
  API cassDocAPI

  // zero-arg cons required
  public CassDocJcrRepository()
  {

  }

  @Override
  public String getDescriptor(String key)
  {
    Value v = getDescriptorValue(key);
    String s = (v == null) ? null : v.getString();
  }

  @Override
  public String[] getDescriptorKeys()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Value getDescriptorValue(String key)
  {

  }

  @Override
  public Value[] getDescriptorValues(String key)
  {
  }

  @Override
  public boolean isSingleValueDescriptor(String key)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isStandardDescriptor(String key)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Session login() throws LoginException, RepositoryException
  {
    Session session = new CassDocJcrSession(repo:this)
  }

  @Override
  public Session login(Credentials arg0, String space) throws LoginException, NoSuchWorkspaceException, RepositoryException
  {
    Session session = new CassDocJcrSession(repo:this)
  }

  @Override
  public Session login(Credentials arg0) throws LoginException, RepositoryException
  {
    Session session = new CassDocJcrSession(repo:this)
  }

  @Override
  public Session login(String arg0) throws LoginException, NoSuchWorkspaceException, RepositoryException
  {
    Session session = new CassDocJcrSession(repo:this)
  }


}
