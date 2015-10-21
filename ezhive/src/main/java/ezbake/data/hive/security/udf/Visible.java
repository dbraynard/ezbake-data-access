package ezbake.data.hive.security.udf;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import ezbake.data.hive.security.utils.VisibilityChecker;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import static ezbake.thrift.ThriftUtils.deserializeFromBase64;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.data.hive.security.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;

public class Visible extends UDF {
    public BooleanWritable evaluate(final Text visibility, 
				    String token, 
				    String path) {
	ensureChecker(path);
	boolean result = _evaluate(visibility, token);
	return new BooleanWritable(result);
    }

    private boolean _evaluate(final Text visibility, String token) {
	if (token == null) { return false; } 
	try {
	    if (checker.check(deserializeFromBase64(EzSecurityToken.class,
						    token),
			      deserializeFromBase64(Visibility.class, 
						    visibility.toString()))) {
		logger.trace("token valid: {}", token);
		return true;
	    } else {
		logger.error("token not valid: {}", token);
		return false;
	    }
	} catch (TException e) {
	    logger.error("trouble deserializing vis {} or token {}",
			 visibility, token, e);
	    return false;
	}
    }
    
    // exposed for testing
    void setChecker(VisibilityChecker checker) { this.checker = checker; }

    private void ensureChecker(String path) {
	Path[] p = new Path[]{FileSystems.getDefault().getPath(path)};
	if (checker == null)
	    checker = new VisibilityChecker(
		        new EzbakeSecurityClient(
			  ConfigUtils.getProperties(p)));
    }

    private VisibilityChecker checker = null;
    private static final Logger logger = LoggerFactory.getLogger(Visible.class);
}
