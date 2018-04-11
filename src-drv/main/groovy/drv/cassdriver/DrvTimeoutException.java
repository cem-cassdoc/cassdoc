package drv.cassdriver;

public class DrvTimeoutException extends RuntimeException
{
  private static final long serialVersionUID = 1L;

  public DrvTimeoutException()
  {
    super();
  }

  public DrvTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
  {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DrvTimeoutException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public DrvTimeoutException(String message)
  {
    super(message);
  }

  public DrvTimeoutException(Throwable cause)
  {
    super(cause);
  }
}
