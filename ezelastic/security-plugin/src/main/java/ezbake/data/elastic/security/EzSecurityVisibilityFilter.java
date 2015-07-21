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

package ezbake.data.elastic.security;

import static ezbake.thrift.ThriftUtils.deserializeFromBase64;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import static org.elasticsearch.index.fielddata.ScriptDocValues.Strings;
import org.elasticsearch.script.AbstractSearchScript;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.security.permissions.PermissionEvaluator;
import ezbake.thrift.serializer.Base64Serializer;
import ezbake.thrift.serializer.CachingSerializer;

public final class EzSecurityVisibilityFilter extends AbstractSearchScript {
    public static final String VISIBILITY_FIELD_PARAM = "visibilityField";
    public static final String REQUIRED_PERMISSIONS_PARAM = "requiredPermissions";
    public static final String AUTHS_PARAM = "auths";

    private final ESLogger logger;

    private final String visibilityField;
    private final Authorizations authorizations;
    private final Set<Permission> requiredPermissions;
    private final PermissionEvaluator evaluator;
    private final CachingSerializer<String> serializer;

    public EzSecurityVisibilityFilter(Map<String, Object> params, ESLogger logger) {
        this.logger = logger;

        if (params == null) {
            throw new IllegalArgumentException("Script parameters may not be null");
        }

        visibilityField = (String) params.get(VISIBILITY_FIELD_PARAM);
        if (StringUtils.isEmpty(visibilityField)) {
            throw new IllegalArgumentException("Visibility must be given");
        }

        final String requiredPermsParam = (String) params.get(REQUIRED_PERMISSIONS_PARAM);
        if (StringUtils.isEmpty(requiredPermsParam)) {
            throw new IllegalArgumentException("Required permissions must be given");
        }

        final String[] requiredPermNamesArray = StringUtils.split(requiredPermsParam, ',');
        final List<String> requiredPermNames = Lists.newArrayList(requiredPermNamesArray);

        final List<Permission> requiredPermsList = Lists.transform(
                requiredPermNames, new Function<String, Permission>() {
                    @Override
                    public Permission apply(String input) {
                        return Permission.valueOf(input);
                    }
                });

        requiredPermissions = EnumSet.copyOf(requiredPermsList);

        final String authsBase64 = (String) params.get(AUTHS_PARAM);
        try {
            authorizations = deserializeFromBase64(Authorizations.class, authsBase64);
        } catch (final TException e) {
            final String errMsg = "Could not deserialize authorizations parameter to Authorizations Thrift";
            this.logger.error(errMsg, e);
            throw new IllegalArgumentException(errMsg, e);
        }

        evaluator = new PermissionEvaluator(authorizations);
        serializer = new CachingSerializer<>(new Base64Serializer());
    }

    @Override
    public Object run() {
        final String visibilityBase64 = getStringField(visibilityField);
        if (StringUtils.isEmpty(visibilityBase64)) {
            logger.error("Visibility field {} is missing or empty", visibilityField);
            return false;
        }

        Visibility docVisibility;
        Set<Permission> userPerms;
        try {
            docVisibility = serializer.deserialize(Visibility.class, visibilityBase64);
            userPerms = evaluator.getPermissions(docVisibility);
        } catch (TException e) {
            logger.error("Document visibility deserialization failed.", e);
            return false;
        }

        boolean hasAccess = true;
        for (Permission requiredPerm : requiredPermissions) {
            if (!userPerms.contains(requiredPerm)) {
                hasAccess = false;
                break;
            }
        }

        this.logger.debug(
                "User (with authorizations {} and permissions {}) {} have required permissions {} to document with "
                        + "visibility {} from base64 \"{}\"", authorizations, userPerms,
                hasAccess ? "does" : "does not", requiredPermissions, docVisibility, visibilityBase64);

        return hasAccess;
    }

    private String getStringField(String fieldName) {
        final Strings docValues = (Strings) doc().get(fieldName);
        if (docValues == null) {
            logger.warn("Document didn't contain '" + fieldName + '\'');
            return null;
        }

        final List<String> values = docValues.getValues();
        if (values == null || values.isEmpty()) {
            logger.warn("Document contained no values in '" + fieldName + '\'');
            return null;
        }

        return values.get(0).toString();
    }
}
