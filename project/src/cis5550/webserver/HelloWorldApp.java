package cis5550.webserver;

import static cis5550.webserver.Server.*;
class HelloWorldApp {
    public static void main(String args[]) {
        port(8080);
        securePort(443);
        get("/hello/:name", (req,res) ->
        { return "Hello "+req.params("name"); } );
        get("/", (req, res) -> {return "Hello World - This is Xuanhe";});
    }
}