package org.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public final class ShellRunner {
    private ShellRunner() {}
    public static String run(List<String> command) {
        System.out.println("$ " + String.join(" ", command));
        try {
            Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
            StringBuilder out = new StringBuilder();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                for (String line; (line = r.readLine()) != null;) { out.append(line).append(System.lineSeparator()); System.out.println(line); }
            }
            if (p.waitFor() != 0) System.out.println("Command exited with status " + p.exitValue());
            return out.toString();
        } catch (Exception e) { System.out.println("Unable to run command: " + e.getMessage()); return ""; }
    }
    public static void runInteractive(List<String> command) {
        System.out.println("$ " + String.join(" ", command));
        try {
            Process p = new ProcessBuilder(command).inheritIO().start();
            p.waitFor();
        } catch (Exception e) { System.out.println("Unable to run interactive command: " + e.getMessage()); }
    }
}
