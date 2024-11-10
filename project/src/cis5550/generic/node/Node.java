package cis5550.generic.node;

public interface Node {

    void ip(String ip);

    void port(int port);

    void heartbeat();

    boolean isExpired();

    String hyperlink();

    String id();

    String ip();

    int port();

    String asRow();

    String asIpPort();

    String asHtml();
}
