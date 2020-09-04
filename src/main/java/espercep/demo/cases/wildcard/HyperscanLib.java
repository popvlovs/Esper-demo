package espercep.demo.cases.wildcard;

import espercep.demo.matcher.WildcardTrieMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HyperscanLib {


    public static void main(String[] args) {
        try {
            String[] wildcards = new String[] {
                    "*SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run*",
                    "*\\Environment\\UserInitMprLogonScript*",
                    "*Software\\Microsoft\\Windows NT\\CurrentVersion\\Winlogon\\Userinit*",
                    "*Policies\\Explorer\\Run*",
                    "*\\CurrentVersion\\Winlogon\\Notify*"
            };
            String[] exps = new String[]{
                    ".*SOFTWARE\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\Run.*",
                    ".*\\\\Environment\\\\UserInitMprLogonScript.*",
                    ".*Software\\\\Microsoft\\\\Windows NT\\\\CurrentVersion\\\\Winlogon\\\\Userinit.*",
                    ".*Policies\\\\Explorer\\\\Run.*",
                    ".*\\\\CurrentVersion\\\\Winlogon\\\\Notify.*",
                    "Microsoft"
//                    ".*Software\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\RunServices.*",
//                    ".*\\\\CurrentVersion\\\\ShellServiceObjectDelayLoad.*",
//                    ".*SYSTEM\\\\CurrentControlSet\\\\Control\\\\Session Manager\\\\BootExecute.*",
//                    ".*Software\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\RunServicesOnce.*",
//                    ".*CurrentVersion\\\\RunOnce.*",
//                    ".*Software\\\\Microsoft\\\\Windows NT\\\\CurrentVersion\\\\Winlogon\\\\Shell.*",
//                    ".*CurrentVersion\\\\Run.*",
//                    ".*CurrentVersion\\\\Explorer\\\\Shell Folders.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test1\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test2\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test3\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test4\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test5\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test6\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test7\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test8\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test9\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test10\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test11\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test12\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test13\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test14\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test15\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test16\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test17\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test18\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test19\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test20\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test21\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test22\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test23\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test24\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test25\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test26\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test27\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test28\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test29\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\test30\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\测试\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\天意\\\\load.*",
//                    "人生人生人生.*\\\\CurrentVersion\\\\Windows\\\\人生人生人生\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\无常\\\\load.*",
//                    ".*\\\\CurrentVersion\\\\Windows\\\\烦躁\\\\load.*",
//                    "人生人生人生"
            };

            String text = "C:\\ProgramFiles\\Software\\Microsoft\\Windows NT\\CurrentVersion\\Winlogon\\xhell.exe";
            //String text = "C:\\CurrentVersion\\Winlogon\\Notify.exe";

            System.out.println("Start wildcard Test");
            WildcardTrieMatcher wildcardTrieMatcher = new WildcardTrieMatcher(wildcards);
            long startTime = System.currentTimeMillis();
            boolean isMatched = false;
            for (int i = 0; i < 1_000_000; i++) {
                isMatched = wildcardTrieMatcher.match(text);
            }
            System.out.println("End wildcard Test, result: " + isMatched + ", time elapsed " + (System.currentTimeMillis() - startTime) + " ms");

            System.out.println("Start JAVA RE Test");
            List<Pattern> patterns = new ArrayList<>();
            for (String expression : exps) {
                patterns.add(Pattern.compile(expression));
            }
            startTime = System.currentTimeMillis();
            for (int i = 0; i < 1_000_000; i++) {
                for (Pattern pattern : patterns) {
                    Matcher matcher = pattern.matcher(text);
                    isMatched |= matcher.matches();
                }
            }
            System.out.println("End JAVA RE Test, result: " + isMatched + ", time elapsed " + (System.currentTimeMillis() - startTime) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
