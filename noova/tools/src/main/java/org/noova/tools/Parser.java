package org.noova.tools;

import org.checkerframework.checker.regex.qual.Regex;

public class Parser {

    public static String[] imagesToHtml(String images){
        String delimiter = PropertyLoader.getProperty("delimiter.default");
        String[] imageArray = images.split(delimiter);
        String[] htmlArray = new String[imageArray.length];
        for(int i = 0; i < imageArray.length; i++){
            System.out.println("image: " + imageArray[i]);
            htmlArray[i] = "<img src=\"" + imageArray[i] + "\" />";
        }
        return htmlArray;
    }
}
