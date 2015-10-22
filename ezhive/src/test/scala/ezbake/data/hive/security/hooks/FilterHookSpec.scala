package ezbake.data.hive.security.hooks

import org.easymock.Capture
import org.easymock.EasyMock
import org.easymock.EasyMock._

import scala.collection.mutable.Stack
import scala.collection.convert.wrapAll._

import org.scalatest.{PrivateMethodTester,FunSpec}
import org.scalatest.matchers.{Matcher,MatchResult,ShouldMatchers}
import org.scalatest.mock.EasyMockSugar
import ezbake.base.thrift.EzSecurityToken
import ezbake.thrift.ThriftClientPool
import ezbake.security.thrift.EzSecurity
import ezbake.security.client.EzbakeSecurityClient

import ezbake.crypto.utils.EzSSL
import ezbake.crypto.RSAKeyCrypto
import ezbake.configuration.constants.EzBakePropertyConstants
import java.util.{Arrays,HashMap,Properties}

import org.antlr.runtime.CommonToken
import org.apache.hadoop.hive.ql.parse.{ASTNode,HiveParser,HiveSemanticAnalyzerHookContext}
import org.apache.hadoop.conf.Configuration
import scala.language.implicitConversions
import ezbake.data.hive.security.utils.ASTBuilder.{top,ast}
import org.apache.hadoop.hive.ql.lib.Node

class FilterHookSpec extends FunSpec
    with ShouldMatchers
    with EasyMockSugar 
    with PrivateMethodTester {

  describe("FilterHook") {
    it ("should add a visibility check to a select statement without a where clause") {
      withConf(defaultConf, {context =>
        val x = subject
          .preAnalyze(context, query())
        println(x)
          x.should(haveWhereClause(visCheck(defaultConf)))
      })
    }
  }

  //--------------------------------------------------------------------------------

  def subject = new FilterHook

  //--------------------------------------------------------------------------------

  def withConf(
    conf : FilterConf,
    test : HiveSemanticAnalyzerHookContext => Unit
  ) {

    val tup     = mockContext(defaultConf)
    val conf    = tup._1
    val context = tup._2

    replay(conf)
    replay(context)

    test(context)

  }

  //--------------------------------------------------------------------------------

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

  //--------------------------------------------------------------------------------

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

  //--------------------------------------------------------------------------------

  class FilterConf(
    val visFcn : String,
    val visCol : String,
    val tok : String,
    val path : String
  ) {}

  val defaultConf = new FilterConf("visible", "visibility", "abc123", "xyz789")

  def mockContext(fc : FilterConf) = {

    val visFcnVar = "ezbake.visibility_function"
    val visColVar = "ezbake.visibility_column"
    val    tokVar = "ezbake.token"
    val   pathVar = "ezbake.path"

    val   conf    = mock[Configuration]
    val   context = mock[HiveSemanticAnalyzerHookContext]

    context.getConf(     ).andReturn(     conf)
    conf   .get(visFcnVar).andReturn(fc.visFcn)
    conf   .get(visColVar).andReturn(fc.visCol)
    conf   .get(   tokVar).andReturn(   fc.tok)
    conf   .get(  pathVar).andReturn(  fc.path)

    (conf, context)
  }

  def visCheck(fc : FilterConf) =
    ast(top("TOK_WHERE", "WHERE"),
      ast(top("TOK_FUNCTION"),
        ast(top("Identifier", fc.visFcn)),
        ast(top("TOK_TABLE_OR_COL"),
          ast(top("Identifier", fc.visCol))),
        ast(top("StringLiteral", fc.tok)),
        ast(top("StringLiteral", fc.path))))

  def query(where : Option[ASTNode] = None) =
    ast(top("TOK_QUERY"),
      ast(top("TOK_FROM"),
        ast(top("TOK_TABREF"),
          ast(top("TOK_TABNAME"),
            ast(top("Identifier", "foo"))))),
      ast(top("TOK_INSERT"),
        (
          List(
            ast(top("TOK_DESTINATION"),
              ast(top("TOK_DIR"),
                ast(top("TOK_TMP_FILE")))),
            ast(top("TOK_SELECT"),
              ast(top("TOK_SELEXPR"),
                ast(top("TOK_ALLCOLREF"))))
          ) ++ (where match { 
            case Some(w) => List(w);
            case _ => List[ASTNode]()
          })
        ) : _*))
}
