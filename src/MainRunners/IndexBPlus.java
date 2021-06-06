package MainRunners;

public class IndexBPlus {
    private static String input_file;
    private static int input_PageSize;

    public static void main(String[] args) {
        if (args.length != constants.DBLOAD_ARG_COUNT) {
            System.out.println("Error: Incorrect number of arguments were input");
            return;
        }
        if(verifyArgs(args));
        {
            input_file = args[2];
            input_PageSize = Integer.valueOf(args[1]);
        }
        run();
    }

    private static boolean verifyArgs(String[] args)
    {
        return false;
    }

    public static void run()
    {

    }
}
