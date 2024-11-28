package org.noova.indexer;

public class LinkImageEntry {
    private String links;
    private String imgs;

    public LinkImageEntry(String links, String imgs) {
        this.links = links;
        this.imgs = imgs;
    }

    public String getLinks() {
        return links;
    }

    public void setLinks(String links) {
        this.links = links;
    }

    public String getImgs() {
        return imgs;
    }

    public void setImgs(String imgs) {
        this.imgs = imgs;
    }

    @Override
    public String toString() {
        return "Links: " + links + ", Images: " + imgs;
    }
}
