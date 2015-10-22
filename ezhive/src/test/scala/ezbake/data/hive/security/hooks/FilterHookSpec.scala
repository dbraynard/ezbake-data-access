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

class FilterHookSpec extends FilterHookAux {

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

}
