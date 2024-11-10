package cis5550.webserver.parser;

public enum ParserState {
    STATUS_LINE,
    STATUS_LINE_CR,
    HEADER,
    HEADER_CR,
    HEADER_END,
    HEADER_FINISH_CR,
    BODY,
    ERROR,
    FINISHED,
}
