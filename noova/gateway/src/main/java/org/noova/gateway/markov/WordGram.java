package org.noova.gateway.markov;

import  java.util.*;

public class WordGram {
    private String[] myWords;
    private int myHash;

    public WordGram(String[] source, int start, int size) {
        myWords = new String[size];
        System.arraycopy(source, start, myWords, 0, size);
    }

    public String wordAt(int index) {
        if (index < 0 || index >= myWords.length) {
            throw new IndexOutOfBoundsException("bad index in wordAt "+index);
        }
        return myWords[index];
    }

    public int length(){
        int size = myWords.length;
        return size;
    }

    public String toString(){
        String ret = "";
        for(String word:myWords){
            ret = ret += word + " ";
        }
        return ret.trim();
    }

    public boolean equals(Object o) {
        WordGram other = (WordGram) o;
        if(this.length() != (other.length())){
            return false;
        }
        for(int i=0;i<this.length();i++){
            if(!this.wordAt(i).equals(other.wordAt(i))){
                return false;
            }
        }
        return true;
    }

    public WordGram shiftAdd(String word) {
        String[] st = new String[myWords.length];
        for(int i=0;i<myWords.length-1;i++){
            st[i] = myWords[i+1];
        }
        st[myWords.length-1] = word;

        WordGram out = new WordGram(st, 0, myWords.length);
        return out;
    }

    public int hashCode(){
        return toString().hashCode();
    }

}