package org.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public final class ShellRunner {
    public record Result(int exitCode, String output) {}
    private ShellRunner() {}
    public static String run(List<String> command) {
        return runResult(command).output();
    }
    public static Result runResult(List<String> command) {
        System.out.println("$ " + String.join(" ", command));
        try {
            Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
            StringBuilder out = new StringBuilder();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                for (String line; (line = r.readLine()) != null;) { out.append(line).append(System.lineSeparator()); System.out.println(line); }
            }
            int exitCode = p.waitFor();
            if (exitCode != 0) System.out.println("Command exited with status " + exitCode);
            return new Result(exitCode, out.toString());
        } catch (Exception e) { System.out.println("Unable to run command: " + e.getMessage()); return new Result(-1, ""); }
    }
    public static void runInteractive(List<String> command) {
        System.out.println("$ " + String.join(" ", command));
        try {
            Process p = new ProcessBuilder(command).inheritIO().start();
            p.waitFor();
        } catch (Exception e) { System.out.println("Unable to run interactive command: " + e.getMessage()); }
    }
}
