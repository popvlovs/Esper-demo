package espercep.demo.matcher;

import com.gliwka.hyperscan.wrapper.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/6/29
 */
public class HyperscanMatcher {
    private List<Expression> expressions;
    private Database database;

    public HyperscanMatcher(String... patterns) {
        expressions = Arrays.stream(patterns)
                .map(ptn -> new Expression(ptn, EnumSet.of(ExpressionFlag.MULTILINE, ExpressionFlag.CASELESS)))
                .collect(Collectors.toList());
        try {
            database = Database.compile(expressions);
        } catch (CompileErrorException e) {
            database = null;
        }
    }

    public boolean match(String text) {
        try (Scanner scanner = new Scanner()) {
            scanner.allocScratch(database);
            List<Match> matches = scanner.scan(database, text);
            return !matches.isEmpty();
        } catch (IOException e) {
            return false;
        }
    }
}
