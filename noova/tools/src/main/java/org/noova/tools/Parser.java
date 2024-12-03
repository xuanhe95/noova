package org.noova.tools;

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

//    public static String processSingleWord(String rawWord){
//        if(rawWord==null || rawWord.isEmpty()) return rawWord;
//        return rawWord.replaceAll("[^\\p{ASCII}]", "") // non ascii remove
//                .replaceAll("\\s+", " ")             // Normalize spaces
//                .trim();
//    }

    public static String removeAfterFirstPunctuation(String input) {

        String punctuation = ".,!?:;\"'(){}[]-";

        int firstPunctuationIndex = -1;

        for (int i = 0; i < input.length(); i++) {
            if (punctuation.indexOf(input.charAt(i)) != -1) {
                firstPunctuationIndex = i; // should be more memory friendly vs split
                break;
            }
        }

        if (firstPunctuationIndex == -1) {
            return input;
        }

        return input.substring(0, firstPunctuationIndex);
    }

    public static void main(String[] args) {
        String testString = "asd123.jpg";
        String result = removeAfterFirstPunctuation(testString);

        System.out.println("Original: " + testString);
        System.out.println("Processed: " + result);
    }

}
