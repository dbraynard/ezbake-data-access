package ezbake.data.hive.security.utils;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;

public class ASTBuilder {
    public static ASTNode top(String tokName) {
        return top(tokName, tokName);
    }

    public static ASTNode top(String tokName, String value) {
        return top(getTok(tokName), value);
    }

    public static ASTNode top(int tok, String value) {
        return new ASTNode(new CommonToken(tok, value));
    }

    public static ASTNode ast(ASTNode top, ASTNode... children) {
        top = (ASTNode)top.dupNode();
        for (ASTNode c : children) top.addChild(c);
        return top;
    }

    private static int getTok(String name) {
        try {
            return ((Integer)
                    (HiveParser.class.getField(name).get(HiveParser.class)))
                .intValue();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IndexOutOfBoundsException(
                        "couldn't find token " + name + ": " + e.getMessage());
        }
    }
}
