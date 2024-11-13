package org.noova.gateway.markov;

import java.util.*;

public abstract class MarkovModel implements IMarkovModel {
    protected String text;
    protected Random random;

    public MarkovModel() {
        random = new Random();
    }

    public void setTraining(String s) {
        text = s.trim();
    }

    protected ArrayList<String> getFollows(String key){
        ArrayList<String> follows=new ArrayList<String>();
        for(int i = 0; i< text.length()-1; i++){
            String curString = text.substring(i,i+key.length());
            if(curString.equals(key)){
                String nextString = text.substring(i+key.length(),i+1+key.length());
                follows.add(nextString);
            }
        }
        return follows;
    }


    abstract public String getRandomText(int numChars);

}