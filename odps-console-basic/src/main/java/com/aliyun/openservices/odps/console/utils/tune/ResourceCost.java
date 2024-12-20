package com.aliyun.openservices.odps.console.utils.tune;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResourceCost {

    private String formattedString;
    private double cpuValue;
    private double memValue;

    private ResourceCost(String formattedResource, double cpuValue, double memValue) {
        this.formattedString = formattedResource;
        this.cpuValue = cpuValue;
        this.memValue = memValue;
    }

    public static ResourceCost makeUnknownCost(String reason) {
        return new ResourceCost(reason, -1, -1);
    }

    public static ResourceCost fromText(String text) {
        if (text == null) {
            return null;
        }
        Pattern pattern = Pattern.compile(
                "resource cost: cpu ([\\d\\.]+) Core \\* Min, memory ([\\d\\.]+) GB \\* Min",
                Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            String matchedString = matcher.group(0).replace("resource cost:", "").trim();
            double cpuValue = Double.parseDouble(matcher.group(1));
            double memValue = Double.parseDouble(matcher.group(2));
            return new ResourceCost(matchedString, cpuValue, memValue);
        }
        return makeUnknownCost("unknown cost from text");
    }

    public String getFormattedString() {
        return formattedString;
    }

    public double getCpuValue() {
        return cpuValue;
    }

    public double getMemValue() {
        return memValue;
    }
}
