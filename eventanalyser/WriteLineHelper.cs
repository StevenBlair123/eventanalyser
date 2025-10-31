namespace eventanalyser;

public static class WriteLineHelper {
    public static void WriteInfo(String msg) {
        Console.WriteLine(msg);
    }

    public static void WriteWarning(String msg) {
        ConsoleColor foreground = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(msg);
        Console.ForegroundColor = foreground;
    }
}