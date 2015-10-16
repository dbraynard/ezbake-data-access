package ezbake.data.hive.security.hooks;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterHook extends AbstractSemanticAnalyzerHook {
    private static final String TOK_VAR = "ezbake.token";
    private static final String VIS_COL_VAR = "ezbake.visibility_column";
    private static final Logger LOG = LoggerFactory.getLogger(FilterHook.class);

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, 
			      ASTNode ast)
	throws SemanticException {
	System.out.println("before rewrite");
	dump(ast);
	System.out.println("done dumping");
	Configuration conf = context.getConf();
	String token = conf.get(TOK_VAR);
	String visCol = conf.get(VIS_COL_VAR);
	ASTNode newAst = rewriteAST(ast, visCol, token);
	System.out.println("after rewrite");
	dump(newAst);
	System.out.println("done dumping after rewrite");
	return super.preAnalyze(context, newAst);
    }
    
    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context, 
			    List<Task<? extends Serializable>> rootTasks) 
	throws SemanticException {
	super.postAnalyze(context, rootTasks);
    }

    private ASTNode rewriteAST(ASTNode ast, String visCol, String token) {
	// NOTE: We're modifying the ast in place. I think that's okay. -- Josh
	ASTNode result = ast;

	switch (result.getToken().getType()) {
	case HiveParser.TOK_SELECT:
	    addClauseToSelect(result, visibilityAST(visCol, token));
	    //addClauseToSelect(result, trueAST());
	    break;
	default:
	    List<Node> children = ast.getChildren();
	    if (children != null)
		for (Node child : children)
		    if (child instanceof ASTNode)
			rewriteAST((ASTNode)child, visCol, token);
	    break;
	}
	
	return result;
    }

    private void addClauseToSelect(ASTNode select, ASTNode expr) {
	ASTNode whereClause = whereClauseFromSelect(select);

	List<Node> whereChildren = whereClause.getChildren();
	if (whereChildren == null || whereChildren.size() == 0)
	    whereClause.addChild(expr);
	else if (whereChildren.size() == 1) {
	    ASTNode curExpr = (ASTNode)whereChildren.get(0);
	    ASTNode newExpr = (ASTNode)andAST(curExpr, expr);
	    curExpr.setParent(newExpr);
	    whereClause.setChild(0, newExpr);
	} else
	    throw new RuntimeException("too many children in where clause? " + whereChildren.size() + " children");
    }

    private ASTNode whereClauseFromSelect(ASTNode select) {
	ASTNode parent = (ASTNode)select.getParent();
	if (parent != null) {
	    if (parent.getChildren() != null)
		for (Node n : parent.getChildren())
		    if (n instanceof ASTNode)
			if (((ASTNode)n).getToken().getType() == HiveParser.TOK_WHERE)
			    return (ASTNode)n;

	    ASTNode whereClause = 
		new ASTNode(new CommonToken(HiveParser.TOK_WHERE, "WHERE"));
	    parent.addChild(whereClause);
	    return whereClause;
	}
	
	throw new RuntimeException("select clause has no parent!");
    }


    private void dump(ASTNode ast) { dump(ast, 0); }

    private void dump(ASTNode ast, int indent) {
	for (int i = 0; i < indent; i++) System.out.print("  ");
	System.out.println(ast.getToken().getText() + " : " + ast.getToken().getType());

	List<Node> children = ast.getChildren();
	if (children != null)
	    for (Node n : children)
		if (n != null && n instanceof ASTNode)
		    dump((ASTNode)n, indent + 1);
    }

    static ASTNode andAST(ASTNode left, ASTNode right) {
	if (left == null) return right;
	else if (right == null) return left;
	else {
	    ASTNode and = new ASTNode(new CommonToken(HiveParser.KW_AND, "AND"));
	    adaptor.addChild(and, left);
	    adaptor.addChild(and, right);
	    return and;
	}
    }

    private static final CommonTreeAdaptor adaptor = new CommonTreeAdaptor();

    private static ASTNode visibilityAST(String visCol, String token) {	
	ASTNode eq = new ASTNode(new CommonToken(HiveParser.EQUAL, "="));
	ASTNode lhs = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL));
	ASTNode ident = new ASTNode(new CommonToken(HiveParser.Identifier, visCol));
	ASTNode rhs = new ASTNode(new CommonToken(HiveParser.Number, token));
	lhs.addChild(ident);
	adaptor.addChild(eq, lhs);
	adaptor.addChild(eq, rhs);
	return eq;
    }

    // for testing purposes
    // private static ASTNode trueAST(String visibility) {
    // 	ASTNode eq = new ASTNode(new CommonToken(HiveParser.EQUAL, "="));
    // 	ASTNode lhs = new ASTNode(new CommonToken(HiveParser.Number, "1"));
    // 	ASTNode rhs = new ASTNode(new CommonToken(HiveParser.Number, "1"));
    // 	adaptor.addChild(eq, lhs);
    // 	adaptor.addChild(eq, rhs);
    // 	return eq;
    // }
}
