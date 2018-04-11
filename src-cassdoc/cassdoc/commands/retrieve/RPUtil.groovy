package cassdoc.commands.retrieve

import cassdoc.Rel

class RPUtil {

  static List<Rel> getAllRels(RowProcessor rp) {
    List<Rel> rels = []
    Object[] row = null
    while (row = rp.nextRow()) {
      rels.add((Rel)row[0])
    }
    return rels
  }
}
