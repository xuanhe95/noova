package org.noova.gateway.trie;

import java.util.List;

public interface ITrie {
    public void insert(String word, Object value);
    public boolean contains(String word);
    public List<String> getWordsWithPrefix(String prefix, int limit);

    public boolean containsAfter(String prefix, char c);

    public boolean startsWith(String prefix);


}
