package MainRunners;

import IndexKeyValues.IntegerKey;
import IndexKeyValues.Value;
import Tree.Root;

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
            input_PageSize = Integer.parseInt(args[1]);
        }
        run(input_file,input_PageSize);
    }

    private static boolean verifyArgs(String[] args)
    {
        return false;
    }

    public static void run(String file, int page_Size)
    {
        IntegerKey key = new IntegerKey(0);
        Value value = new Value(0,0);
        Root<IntegerKey,Value> rootNode = new Root<IntegerKey,Value>(key, value);

        //dbEntityRow entityType = new dbEntityRow();
       // dbBytePage<dbEntityRow> pageType = new dbBytePage<>(options.pageSize,entityType);
        //dbEntityLoader<dbEntityRow,dbBytePage<dbEntityRow>> loader = new dbEntityLoader<>(options.file, pageType, entityType);
    }
}
