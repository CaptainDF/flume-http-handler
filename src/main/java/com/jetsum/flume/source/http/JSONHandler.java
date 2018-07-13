package com.jetsum.flume.source.http;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.util.*;

public class JSONHandler implements HTTPSourceHandler {

    private static final String FORWARD_HEADERS = "forwardHeaders";
    private static final Logger logger = LoggerFactory.getLogger(JSONHandler.class);
    private static Set<String> forwardHeaders = new HashSet<String>();
    @Override
    public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
        Map<String,String> eventHeaders = new HashMap<String,String>();
        Enumeration requestHeaders = request.getHeaderNames();
        while (requestHeaders.hasMoreElements()) {
            String header = (String) requestHeaders.nextElement();
            if (forwardHeaders.contains(header)) {
                eventHeaders.put(header, request.getHeader(header));
            }
        }
        BufferedReader reader = request.getReader();
        List<Event> eventList = new ArrayList<Event>(1);
        StringBuffer lineBuffer = new StringBuffer();
        boolean tag;
        do {
            lineBuffer.append(reader.readLine());
        } while (tag = reader.read() != -1);
        if (lineBuffer != null) {
            Event event = new JSONEvent();
            event.setBody(lineBuffer.toString().getBytes());
            event.setHeaders(eventHeaders);
            eventList.add(event);
            logger.info("========= Event body:" + new String(event.getBody()) + "==============");
        }
        return eventList;
    }

    @Override
    public void configure(Context context) {
        String confForwardHeaders = context.getString(FORWARD_HEADERS);
        if (confForwardHeaders != null) {
            if (forwardHeaders.addAll(Arrays.asList(confForwardHeaders.split(",")))) {
                logger.debug("forwardHeaders=" + forwardHeaders);
            } else {
                logger.error("error to get forward headers from " + confForwardHeaders);
            }
        } else {
            logger.debug("no forwardHeaders");
        }
    }

}
