package org.noova.tools;

public class CapitalizeTitle {

    public static String toTitleCase(String input) {
        if (input == null || input.isEmpty()) {
            return input; // Handle null or empty strings
        }

        StringBuilder titleCase = new StringBuilder(input.length());
        boolean capitalizeNext = true;

        for (char c : input.toCharArray()) {
            if (Character.isWhitespace(c)) {
                capitalizeNext = true;
                titleCase.append(c);
            } else if (capitalizeNext) {
                titleCase.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                titleCase.append(Character.toLowerCase(c));
            }
        }

        return titleCase.toString();
    }

    public static void main(String[] args) {
        String input = "java method to MAKE a string a TITLE!";
        String result = toTitleCase(input);
        System.out.println(result); // Output: "Java Method To Make A String A Title!"
    }
}