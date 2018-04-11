package cwdrg.util.json;

import java.math.BigInteger;

public class CassUtil
{
  /**
   * Since hashing should produce a fairly even distribution, we should be able to estimate the number of
   * rows by SELECTING LIMIT 10 rowkey tokens. Pass the starting token, the ending token, and the number
   * returned (0 to LIMIT). Remember that if you just select LIMIT(10) wiht no start token, the start token
   * is LONG.MIN_VALUE.
   * 
   * @param startToken
   * @param stopToken
   * @param rangeCount
   * @return
   */
  public static Long estimateRowCount(Long startToken, Long stopToken, Long rangeCount)
  {
    BigInteger start = BigInteger.valueOf(startToken);
    BigInteger stop = BigInteger.valueOf(stopToken);
    BigInteger range = stop.subtract(start).abs();
    BigInteger longmax = BigInteger.valueOf(Long.MAX_VALUE);
    BigInteger subrangeCount = longmax.divide(range);
    if (subrangeCount.equals(0L)) {
      subrangeCount = BigInteger.valueOf(1L);
    } else {
      subrangeCount = subrangeCount.multiply(BigInteger.valueOf(2L));
    }
    Long estimate = subrangeCount.multiply(BigInteger.valueOf(rangeCount)).longValue();
    return estimate;
  }

}
