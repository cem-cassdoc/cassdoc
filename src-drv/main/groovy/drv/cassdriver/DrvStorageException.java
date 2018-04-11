package drv.cassdriver;

/**
 * General Storage exception class indicates problems in storage engine. Use this to avoid having to throw base {@link RuntimeException}.
 * TODO: refactor this into a common-storage project.
 */
public class DrvStorageException extends RuntimeException
{
  private static final long serialVersionUID = 1L;

  public DrvStorageException()
  {
    super();
  }

  public DrvStorageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
  {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DrvStorageException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public DrvStorageException(String message)
  {
    super(message);
  }

  public DrvStorageException(Throwable cause)
  {
    super(cause);
  }
}
