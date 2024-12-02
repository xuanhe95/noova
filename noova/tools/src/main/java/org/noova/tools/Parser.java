package org.noova.tools;


import java.io.UnsupportedEncodingException;

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

    public static String processWord (String rawText){
        // [Process Helper] remove non-eng word + normalize space

        if(rawText==null || rawText.isEmpty()) return rawText;

        // preserve unicode
//        return rawText.replaceAll("[^\\p{Print}]", " ") // non printable rm
//                .replaceAll("\\s+", " ")             // Normalize spaces
//                .trim();

        // preserve ascii
        return rawText.replaceAll("[^\\p{ASCII}]", " ") // non ascii remove
                .replaceAll("\\s+", " ")             // Normalize spaces
                .trim();
    }

}
