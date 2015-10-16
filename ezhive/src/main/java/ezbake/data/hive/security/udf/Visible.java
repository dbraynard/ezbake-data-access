package ezbake.data.hive.security.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class Visible extends UDF {
    public BooleanWritable evaluate(final Text s, String compare) {
	System.out.println("comparing " + s + " to " + compare);
	boolean result = _evaluate(s, compare);
	System.out.println("result: " + result);
	return new BooleanWritable(result);
    }

    public boolean _evaluate(final Text s, String compare) {
	if (s == null) { return false; }
	return s.equals(new Text(compare));
    }
}
