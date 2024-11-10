package cis5550.generic.node;

import cis5550.generic.Coordinator;
import cis5550.tools.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WorkerNode implements Node {

    private static final Logger log = Logger.getLogger(WorkerNode.class);

    private final long EXPIRATION_TIME = 15 * 1000;
    private final String id;
    private String ip;
    private int port;

    private long lastHeartbeat = System.currentTimeMillis();
    public WorkerNode(String id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    public long lastHeartbeat() {
        return lastHeartbeat;
    }

    public void heartbeat() {
        lastHeartbeat = System.currentTimeMillis();
    }

    public String id() {
        return id;
    }

    public String ip() {
        return ip;
    }

    public int port() {
        return port;
    }

    public void ip(String ip) {
        this.ip = ip;
    }

    public void port(int port) {
        this.port = port;
    }

    public boolean isExpired() {
        return (System.currentTimeMillis()) - lastHeartbeat > EXPIRATION_TIME;
    }

    @Override
    public String asRow() {
        return id + "," + asIpPort();
    }

    public String asIpPort(){
        return ip + ":" + port;
    }

    public String hyperlink() {
        return "<a href=\"http://" + asIpPort() + "\">" + "Manage" + "</a>";
    }


    /*
    * This method returns the worker node as an HTML table row
     */
    public String asHtml() {
        Date date = new Date(lastHeartbeat);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = formatter.format(date);
        StringBuilder builder = new StringBuilder();

        builder.append("<tr>")
                .append("<td>").append(id).append("</td>")
                .append("<td>").append(ip).append("</td>")
                .append("<td>").append(port).append("</td>").
                append("<td>").append(hyperlink()).append("</td>")
                .append("<td>").append(time).append("</td>")
                .append("</tr>");

        return builder.toString();
    }


}
