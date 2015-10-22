package ezbake.data.hive.security.hooks

import org.scalatest.{PrivateMethodTester,FunSpec}
import org.scalatest.matchers.{Matcher,MatchResult,ShouldMatchers}
import org.scalatest.mock.EasyMockSugar

class BaseSpecAux extends FunSpec
    with ShouldMatchers
    with EasyMockSugar 
    with PrivateMethodTester {} 
