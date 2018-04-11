package drv.cassdriver;

/**
 * General Storage exception class indicates problems in storage engine. Use this to avoid having to throw base {@link RuntimeException}.
 * TODO: refactor this into a common-storage project.
 */
public class DrvRetrievalException extends RuntimeException
{
  private static final long serialVersionUID = 1L;

  public DrvRetrievalException()
  {
    super();
  }

  public DrvRetrievalException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
  {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DrvRetrievalException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public DrvRetrievalException(String message)
  {
    super(message);
  }

  public DrvRetrievalException(Throwable cause)
  {
    super(cause);
  }
}
