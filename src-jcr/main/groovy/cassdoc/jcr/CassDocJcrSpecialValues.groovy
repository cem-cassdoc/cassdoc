package cassdoc.jcr

import cassdoc.Rel

/**
 * This 
 * 
 * @author cowardlydragon
 *
 */
class CassDocJcrNewChildDoc extends CassDocJcrNode {
  CassDocJcrNode node = null
}

class CassDocJcrOverlay extends CassDocJcrNode{
  CassDocJcrNode node = null
}

class CassDocJcrAddRel extends CassDocJcrNode{
  Rel rel = null
}

class CassDocJcrDelRel extends CassDocJcrNode{
  Rel rel = null
}

class CassDocJcrObjectValue extends CassDocJcrNode{
  String json
}




