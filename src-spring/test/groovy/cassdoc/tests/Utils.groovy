package cassdoc.tests

class Utils {

  static stripKeys(Object item, String key) {
    if (item instanceof Map) {
      Map map = item
      if (map == null) return
      if (!map.containsKey(key)) return
      map.entrySet().each {
        if (it.value instanceof List || it.value instanceof Map) {
          stripKeys(it.value,key)
        }
      }
      if (map.containsKey(key)) {
        map.remove(key)
      }
    } else if (item instanceof List) {
      List list = item
      list.each {
        if (it instanceof Map || it instanceof List) stripKeys(it,key)
      }
    }
  }
}
