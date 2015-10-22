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
import ezbake.data.hive.security.aux.HiveSecurityMatchers

class FilterHookAux extends BaseSpecAux 
    with HiveSecurityMatchers {

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
