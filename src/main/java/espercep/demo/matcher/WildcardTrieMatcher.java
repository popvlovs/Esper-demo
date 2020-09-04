package espercep.demo.matcher;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * POJ 1816
 *
 * @author yitian_song 2020/6/28
 */
public class WildcardTrieMatcher {

    private List<TrieNode> trieRoots = new ArrayList<>();

    public WildcardTrieMatcher(String... wildcards) {
        Arrays.stream(wildcards).forEach(this::insert);
    }

    public boolean match(String text) {
        for (TrieNode root : trieRoots) {
            if (find(root, text, 0)) {
                return true;
            }
        }
        return false;
    }

    public WildcardTrieMatcher add(String... wildcards) {
        Arrays.stream(wildcards).forEach(this::insert);
        return this;
    }

    private void insert(String pattern) {
        List<TrieNode> curNodes = trieRoots;
        for (int i = 0; i < pattern.length(); i++) {
            char charAt = pattern.charAt(i);
            TrieNode node;
            if (charAt == WildcardKeyword.ASTERISK.symbol
                    || charAt == WildcardKeyword.QUESTION_MARK.symbol) {
                // case * or ?
                node = new WildcardTrieNode(WildcardKeyword.parseFrom(charAt));
            } else {
                // case char
                node = new CharTrieNode(charAt);
            }

            if (curNodes.contains(node)) {
                curNodes = curNodes.get(curNodes.indexOf(node)).getChildren();
            } else {
                curNodes.add(node);
                curNodes = node.getChildren();
            }
        }
    }

    /**
     * 非递归实现
     * 感觉有点慢，暂时不知道慢在哪里 130 ms -> 1200 ms
     */
    private boolean findNonRecursive(TrieNode node, String text, int charIdx) {
        if ((charIdx < 0) || (charIdx >= text.length())) {
            return false;
        }

        Stack<TrieNode> nodeStack = new Stack<>();
        nodeStack.push(node);
        Stack<Integer> idxStack = new Stack<>();
        idxStack.push(charIdx);

        while (!nodeStack.isEmpty()) {
            TrieNode curNode = nodeStack.pop();
            Integer curIdx = idxStack.pop();
            if ((curIdx < 0) || (curIdx >= text.length())) {
                continue;
            }

            char charAt = text.charAt(curIdx);
            if (curNode.matchChar(charAt)) {
                // case ? or char-match
                if (!curNode.hasChild()) {
                    return true;
                }
                for (int i = curNode.children.size() - 1; i >= 0; --i) {
                    nodeStack.push(curNode.children.get(i));
                    idxStack.push(curIdx + 1);
                }
            } else if (curNode.isAsterisk()) {
                // case *
                if (!curNode.hasChild()) {
                    return true;
                }
                for (int i = curNode.children.size() - 1; i >= 0; --i) {
                    // child must be char
                    for (int nextCharIdx = text.length() - 1; nextCharIdx >= curIdx; --nextCharIdx) {
                        nodeStack.push(curNode.children.get(i));
                        idxStack.push(nextCharIdx);
                    }
                }
            }
        }
        return false;
    }

    private boolean find(TrieNode node, String text, int charIdx) {
        if ((charIdx < 0) || (charIdx >= text.length())) {
            return false;
        }
        char charAt = text.charAt(charIdx);
        if (node.matchChar(charAt)) {
            // case ? or char-match
            if (!node.hasChild()) {
                return true;
            }
            for (TrieNode child : node.children) {
                if (find(child, text, charIdx + 1)) {
                    return true;
                }
            }
            return false;
        } else if (node.isAsterisk()) {
            // case *
            if (!node.hasChild()) {
                return true;
            }
            for (TrieNode child : node.children) {
                // child must be char
                for (int nextCharIdx = charIdx; nextCharIdx < text.length(); ++nextCharIdx) {
                    if (find(child, text, nextCharIdx)) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            // case not match
            return false;
        }
    }

    private enum WildcardKeyword {
        ASTERISK('*'),
        QUESTION_MARK('?');

        private char symbol;

        WildcardKeyword(char symbol) {
            this.symbol = symbol;
        }

        public static WildcardKeyword parseFrom(char wildcardChar) {
            if (wildcardChar == ASTERISK.symbol) {
                return ASTERISK;
            } else if (wildcardChar == QUESTION_MARK.symbol) {
                return QUESTION_MARK;
            } else {
                return null;
            }
        }
    }

    private abstract class TrieNode {
        private List<TrieNode> children = new ArrayList<>();

        public List<TrieNode> getChildren() {
            return children;
        }

        boolean hasChild() {
            return children != null && !children.isEmpty();
        }

        abstract boolean matchChar(char textChar);

        abstract boolean isAsterisk();
    }

    private class WildcardTrieNode extends TrieNode {
        private WildcardKeyword keyword;

        public WildcardTrieNode(WildcardKeyword keyword) {
            this.keyword = keyword;
        }

        @Override
        boolean matchChar(char textChar) {
            return keyword == WildcardKeyword.QUESTION_MARK;
        }

        @Override
        boolean isAsterisk() {
            return keyword == WildcardKeyword.ASTERISK;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (!(o instanceof WildcardTrieNode)) return false;

            WildcardTrieNode that = (WildcardTrieNode) o;

            return new EqualsBuilder()
                    .append(keyword, that.keyword)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(keyword)
                    .toHashCode();
        }
    }

    private class CharTrieNode extends TrieNode {
        private char value;

        public CharTrieNode(char value) {
            this.value = value;
        }

        public boolean matchChar(char textChar) {
            return value == textChar;
        }

        public boolean isAsterisk() {
            return false;
        }

        public char getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (!(o instanceof CharTrieNode)) return false;

            CharTrieNode that = (CharTrieNode) o;

            return new EqualsBuilder()
                    .append(value, that.value)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(value)
                    .toHashCode();
        }
    }
}
