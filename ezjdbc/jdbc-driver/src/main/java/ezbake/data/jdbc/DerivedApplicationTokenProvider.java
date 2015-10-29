/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */
package ezbake.data.jdbc;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.security.common.core.EzSecurityClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;

/**
 * A token provider that provides app tokens from a security client based on an original token. Calls
 * {@link ezbake.security.common.core.EzSecurityClient#fetchDerivedTokenForApp()} to
 * obtain the app token.
 */
class DerivedApplicationTokenProvider implements Provider<EzSecurityToken>
{

    private static final Logger logger = LoggerFactory.getLogger(DerivedApplicationTokenProvider.class);
    private EzSecurityClient securityClient;
    
    private EzSecurityToken originalToken;

    /**
     * Constructs a new provider that gets a derived app token from the security client based on the original passed in.
     *
     * @param securityClient security client
     * @param token original token
     */
    public DerivedApplicationTokenProvider(EzSecurityClient securityClient, EzSecurityToken token)
    {
        this.securityClient = securityClient;
        originalToken = token;
    }

    @Override
    public EzSecurityToken get()
    {
        EzSecurityToken token = null;
        try
        {
            token = securityClient.fetchDerivedTokenForApp(originalToken, null);
        }
        catch (EzSecurityTokenException e)
        {
            logger.error("Couldn't get app token from security client", e);
        }

        return token;
    }
}
