package com.infochimps.hivehold.udf;

import java.nio.file.Path;
package com.infochimps.hivehold.utils.ConfigUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class Visible extends UDF {
    public BooleanWritable evaluate(final Text s, String compare) {
	boolean result = _evaluate(s, compare);
	return new BooleanWritable(result);
    }

    public boolean _evaluate(final Text visibility, String token) {
	if (token == null) { return false; }
	ensureChecker();
	return checker.check(deserializeFromBase64(EzSecurityToken.class, token),
			     deserializeFromBase64(Visibility.class, visibility.toString()));
    }
    
    // exposed for testing
    void setChecker(VisibilityChecker checker) { this.checker = checker; }

    private void ensureChecker() {
	Path[] p = new Path[]{};
	if (checker == null)
	    checker = new VisibilityChecker(new EzbakeSecurityClient(p));
    }

    private VisibilityChecker checker = null;
}
