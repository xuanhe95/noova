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

        if(rawText!=null || !rawText.isEmpty()) return rawText;
//        return rawText.replaceAll("[^a-zA-Z0-9\\s]", " ")
//                .replaceAll("\\s+", " ").trim();

//        rawText = org.jsoup.Jsoup.parse(rawText).text();
//        rawText = java.text.Normalizer.normalize(rawText, java.text.Normalizer.Form.NFKC);

        // Remove unwanted characters and normalize space
        return rawText.replaceAll("[^\\p{ASCII}]", " ") // non ascii remove
                .replaceAll("[^a-zA-Z0-9\\s]", " ")
//                .replace("Â©", " ")  // Replace misinterpreted ©
//                .replace("â€™", " ") // Replace misinterpreted apostrophe
//                .replace("â€œ", " ") // Replace misinterpreted opening quote
//                .replace("â€", " ")  // Remove non-ASCII printable chars
                .replaceAll("\\s+", " ")             // Normalize spaces
                .trim();
    }

}
