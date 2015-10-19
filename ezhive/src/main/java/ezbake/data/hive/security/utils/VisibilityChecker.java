package ezbake.data.hive.security.utils;

import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Map;

public class VisibilityChecker {
    public VisibilityChecker(EzbakeSecurityClient securityClient) {
	this.securityClient = securityClient;
    }

    public boolean check(EzSecurityToken token, Visbility vis) {
	
	// Check the signature, expiry, and to see whether the token
	// was issued for this security ID. (The Security ID of the
	// Hive plugin should be specified via the
	// application-specific config passed to it.)
	try {
	    securityClient.validateReceivedToken(token);
	} catch (EzSecurityTokenException e) {
	    logger.error("invalid token used for authentication: {}", token, e);
	    return false;
	}

	// Make sure we have some authorizations, to avoid NPEs.
	if (!token.isSetAuthorizations()) {
	    logger.error("token has no authorizations: {}", token);
	}
    }

    private Map cachedKeys;

    private static final Logger logger = 
	LoggerFactory.getLogger(VisibilityChecker.class);
}
