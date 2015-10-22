package ezbake.data.hive.security.aux

import org.apache.hadoop.hive.ql.parse.ASTNode
import org.apache.hadoop.hive.ql.lib.Node
import scala.collection.convert.wrapAll._

object NodeAux {
  def nodesEquivalent(n1 : Node, n2 : Node) : Boolean = 
    n1.getClass.getName.equals(n2.getClass.getName) &&
    ((n1,n2) match {
      case (a1 : ASTNode, a2 : ASTNode) => astsEquivalent(a1, a2)
      case _ => n1.equals(n2)
    })

  def astsEquivalent(n1 : ASTNode, n2 : ASTNode) : Boolean =
    n1.getToken().getType() == n2.getToken().getType() &&
    n1.getToken().getText() == n2.getToken().getText() &&
    ((n1.getChildren == null) && (n2.getChildren == null)) ||
    (n1.getChildren.zip(n2.getChildren).forall(c => nodesEquivalent(c._1,c._2)))
}
