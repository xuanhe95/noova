package org.noova.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class ParseText {
    public static void main(String[] args) {
        try {
            // Fetch the Wikipedia page
            String url = "https://en.wikipedia.org/wiki/Syrian_opposition";
            Document doc = Jsoup.connect(url).get();

            System.out.println("allContent");
            // Parse all visible text from the body
            //String allContent = parseAllVisibleText(doc);
            //parsePageLinks(doc.body().toString(),"https://en.wikipedia.org:443");

            doc.select("script, style, .popup, .ad, .banner, [role=dialog], footer, nav, aside, .sponsored, " +
                    ".advertisement, iframe, span[data-icid=body-top-marquee], div[class^=ad-]").remove();
            String parsedText = parseVisibleText(doc.body());
            System.out.println(parsedText);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String parseVisibleText(Element element) {
        element.select("script, style").remove(); // Remove unwanted tags upfront
//        return element.text();
        StringBuilder textBuilder = new StringBuilder();

        // Recursively extract text, ignoring script and style tags
        for (Element child : element.children()) {
//            String tagName = child.tagName();
//            if (tagName.equals("script") || tagName.equals("style")) {
//                continue; // Skip non-visible content
//            }
            if (child.children().isEmpty()) {
                String text = cleanText(child.ownText());
                if (!text.isEmpty()) {
                    textBuilder.append(text).append("\n");
                }
            } else {
                textBuilder.append(parseVisibleText(child)).append(" ");
            }
        }
//
        return textBuilder.toString().trim();
    }

    public static String cleanText(String text) {
        // Remove empty brackets
        text = text.replaceAll("\\[\\s*\\]", " ");
        // Normalize whitespace
        text = text.replaceAll("\\s{2,}", " ").trim();
        // Skip very short fragments
        if (text.length() < 3) {
            return "";
        }
        return text;
    }
}
