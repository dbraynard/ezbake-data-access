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

package ezbake.data.postgres;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.security.common.core.EzSecurityClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;

/**
 * A token provider that provides proxied user tokens from a security client. Calls
 * {@link ezbake.security.common.core.EzSecurityClient#fetchTokenForProxiedUser()} (int, int)} to obtain the token from
 * the current Spring Security context.
 */
class SpringSecurityContextTokenProvider implements Provider<EzSecurityToken> {
    private static final Logger logger = LoggerFactory.getLogger(SpringSecurityContextTokenProvider.class);
    private EzSecurityClient securityClient;

    /**
     * Constructs a new provider that gets proxied user tokens from the security client.
     *
     * @param securityClient security client
     */
    public SpringSecurityContextTokenProvider(EzSecurityClient securityClient) {
        this.securityClient = securityClient;
    }

    @Override
    public EzSecurityToken get() {
        EzSecurityToken token = null;

        try {
            token = securityClient.fetchTokenForProxiedUser();
        } catch (EzSecurityTokenException e) {
            logger.error("Couldn't get proxied user token from security client", e);
        }

        return token;
    }
}