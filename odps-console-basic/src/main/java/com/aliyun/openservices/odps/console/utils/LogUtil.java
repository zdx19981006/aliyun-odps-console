package com.aliyun.openservices.odps.console.utils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class LogUtil {

    public static void sendFallbackLog(ExecutionContext ctx, String command, String msg, Exception e) {
        try {
            Map<String, String> msgMap = new HashMap<>();
            msgMap.put("command", command);
            msgMap.put("msg", msg);
            msgMap.put("exception", e == null? "": ExceptionUtils.getFullStackTrace(e));
            sendDeprecatedLogger(ctx, msgMap);
        } catch (Exception ignore) {
        }
//        OdpsDeprecatedLogger.getDeprecatedCalls().put("CMD_FALLBACK ", )
    }

    private static void sendDeprecatedLogger(ExecutionContext ctx, Map<String, String> msgMap)
            throws UnsupportedEncodingException, ODPSConsoleException, OdpsException {
        String resource = ResourceBuilder.buildProjectResource(ctx.getProjectName()) + "/logs";
        String deprecatedLogs =
                new GsonBuilder().disableHtmlEscaping().create().toJson(msgMap);
        byte[] bytes = deprecatedLogs.getBytes("UTF-8");
        ByteArrayInputStream body = new ByteArrayInputStream(bytes);
        Odps odps = OdpsConnectionFactory.createOdps(ctx);
        odps.getRestClient().request(resource, "PUT", null, null, body, bytes.length);
    }
}
