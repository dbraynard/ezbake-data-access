package ezbake.data.hive.security.aux

import org.apache.hadoop.hive.ql.parse.{ASTNode,HiveParser,HiveSemanticAnalyzerHookContext}
import org.scalatest.matchers.{Matcher,MatchResult}
import NodeAux._
import scala.collection.convert.wrapAll._

trait HiveSecurityMatchers {
  class HasWhereClauseMatcher(expected : ASTNode) extends Matcher[ASTNode] {
    def apply(s : ASTNode) = {
      val extracted = whereClauseOf(s)
      val extractedString = extracted match { case Some(s) => s.dump; case None => "None" }
      MatchResult(
        extracted match {
          case Some(w) => nodesEquivalent(expected, w)
          case None => false
        },
        s"""expected --- ${expected.dump} --- but --- ${s.dump} --- contained --- ${extractedString} """,
        s"""${s.dump} contained where clause ${expected.dump}""")
    }

    def whereClauseOf(s : ASTNode) : Option[ASTNode] =
      if (s.getToken.getType == HiveParser.TOK_WHERE)
        Some(s)
      else if (s.getChildren != null) 
        s.getChildren.map(_ match {
          case c : ASTNode => whereClauseOf(c)
          case _ => None
        }).flatten.headOption
      else
        None
  }

  def haveWhereClause(w : ASTNode) = new HasWhereClauseMatcher(w)
}
