package ezbake.data.hive.security.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.ValidityCaveats;
import ezbake.base.thrift.Visibility;
import ezbake.security.permissions.PermissionEvaluator;
import ezbake.base.thrift.Permission;
import java.nio.file.FileSystems;

public class VisibilityChecker {

    public VisibilityChecker(EzbakeSecurityClient securityClient) {
	this.securityClient = securityClient;
    }

    public boolean check(EzSecurityToken token, Visibility vis) {
	return (authsSet(token) && 
		validToken(token) && 
		hasReadPermission(token.getAuthorizations(), vis));
    }

    //-----

    private boolean authsSet(EzSecurityToken tok) {
	if (!tok.isSetAuthorizations()) {
	    logger.error("no authorizations set for token {}", tok);
	    return false;
	} else {
	    logger.trace("authorizations are set for token {}", tok);
	    return true;
	}
    }

    private boolean hasReadPermission(Authorizations auths, Visibility vis) {
	if (evaluator.getPermissions(vis).contains(Permission.READ)) {
	    logger.trace("auths {} include READ-level permission", auths);
	    return true;
	} else {
	    logger.trace("auths {} do not include READ-level permission", auths);
	    return false;
	}
    }

    private boolean validToken(EzSecurityToken token) {

	// First, check to see if this token has already been seen. If
	// it has, we only need to check its expiration.
	if (lastTok != null && lastTok.equals(token) && !expired(token)) {
	    logger.trace("using cached token {}", token);
	    return true;
	}

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

	// If all the checks pass, it's a valid token. Remember it.
	logger.trace("token {} is valid. remembering it.", token);
	lastTok = token;
	setEvaluator(token.getAuthorizations());
	return true;

    }

    //-----

    private boolean expired(EzSecurityToken token) {
	if (token.isSetValidity()) {
	    logger.error("token has no validity information: {}", token);
	    return true;
	}

	ValidityCaveats caveats = token.getValidity();

	if (!caveats.isSetNotAfter()) {
	    logger.error("token has no expiration date: {}", token);
	    return true;
	}

	if (caveats.getNotAfter() < System.currentTimeMillis()) {
	    logger.error("token is expired: {}", token);
	    return true;
	}

	return false;
    }

    private void setEvaluator(Authorizations auths) {
	evaluator = new PermissionEvaluator(auths);
    }

    //-----

    private EzbakeSecurityClient securityClient;
    private EzSecurityToken lastTok = null;
    private PermissionEvaluator evaluator = null;

    //-----

    private static final Logger logger = 
	LoggerFactory.getLogger(VisibilityChecker.class);
}
