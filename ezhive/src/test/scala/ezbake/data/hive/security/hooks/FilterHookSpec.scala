package ezbake.data.hive.security.hooks

import org.easymock.Capture
import org.easymock.EasyMock
import org.easymock.EasyMock._

import scala.collection.mutable.Stack
import scala.collection.JavaConversions._

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


class FilterHookSpec extends FunSpec
    with ShouldMatchers
    with EasyMockSugar 
    with PrivateMethodTester {

  describe("FilterHook") {
    it ("should add a visibility check to a select statement without a where clause") {
      withConf(defaultConf, {context =>
        subject
          .preAnalyze(context, query())
          .should(haveWhereClause(visCheck(defaultConf)))
      })
    }
  }

  //--------------------------------------------------------------------------------

  def subject = new FilterHook

  //--------------------------------------------------------------------------------

  def withConf(conf : FilterConf, test : HiveSemanticAnalyzerHookContext => Unit) {
    val tup     = mockContext(defaultConf)
    val conf    = tup._1
    val context = tup._2

    replay(conf)
    replay(context)

    test(context)
  }

  //--------------------------------------------------------------------------------

  class HasWhereClauseMatcher(expected : ASTNode) extends Matcher[ASTNode] {
    def apply(s : ASTNode) =
      MatchResult(
        whereClauseOf(s) match {
          case Some(w) => expected.equals(whereClauseOf(w))
          case None => false
        },
        s"""expected $expected but $s did not contain that where clause""",
        s"""$s contained where clause $expected""")

    def whereClauseOf(s : ASTNode) : Option[ASTNode] = {
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
  }

  def haveWhereClause(w : ASTNode) = new HasWhereClauseMatcher(w)

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

  def top(tok : Int, name : String = null) =
    new ASTNode(name match {
      case null => new CommonToken(tok)
      case _ : String => new CommonToken(tok, name)
    })

  def ast(top : ASTNode, children : ASTNode*) = {
    for (n <- children) top.addChild(n)
    top
  }

  def visCheck(fc : FilterConf) =
    ast(top(HiveParser.TOK_WHERE, "WHERE"),
      ast(top(HiveParser.TOK_FUNCTION),
        ast(top(HiveParser.Identifier, fc.visFcn)),
        ast(top(HiveParser.TOK_TABLE_OR_COL),
          ast(top(HiveParser.Identifier, fc.visCol))),
        ast(top(HiveParser.StringLiteral, fc.tok)),
        ast(top(HiveParser.StringLiteral, fc.path))))

  def query(where : Option[ASTNode] = None) =
    ast(top(HiveParser.TOK_QUERY),
      ast(top(HiveParser.TOK_FROM),
        ast(top(HiveParser.TOK_TABREF),
          ast(top(HiveParser.TOK_TABNAME),
            ast(top(HiveParser.Identifier, "foo"))))),
      ast(top(HiveParser.TOK_INSERT),
        (
          List(
            ast(top(HiveParser.TOK_DESTINATION),
              ast(top(HiveParser.TOK_DIR),
                ast(top(HiveParser.TOK_TMP_FILE)))),
            ast(top(HiveParser.TOK_SELECT),
              ast(top(HiveParser.TOK_SELEXPR),
                ast(top(HiveParser.TOK_ALLCOLREF))))
          ) ++ (where match { 
            case Some(w) => List(w);
            case _ => List[ASTNode]()
          })
        ) : _*))
}
