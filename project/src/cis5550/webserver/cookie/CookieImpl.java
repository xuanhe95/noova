package cis5550.webserver.cookie;

public class CookieImpl implements Cookie{
    private final String name;
    private String value;
    private boolean httpOnly;
    private boolean secure;
    private SameSite sameSite;


    public CookieImpl(String name, String value){
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public void setValue(String value) {
        this.value = value;
    }

    public boolean isHttpOnly() {
        return httpOnly;
    }

    public void setHttpOnly(boolean httpOnly) {
        this.httpOnly = httpOnly;
    }

    @Override
    public void setSameSite(SameSite sameSite) {
        this.sameSite = sameSite;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public SameSite getSameSite() {
        return sameSite;
    }




    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(name).append("=").append(value);
        if(httpOnly){
            sb.append("; HttpOnly");
        }
        if(secure){
            sb.append("; Secure");
        }
        if(sameSite != null){
            sb.append("; SameSite=").append(sameSite);
        }
        return sb.toString();
    }



}
