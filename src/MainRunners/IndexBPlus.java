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
//        dbIntIndexKey keyType = new dbIntIndexKey(0);
//        dbIndexValue valueType = new dbIndexValue(0,0);
//        bTreeRoot<dbIntIndexKey,dbIndexValue> rootNode = new bTreeRoot<dbIntIndexKey,dbIndexValue>(keyType, valueType);
//
//        dbEntityRow entityType = new dbEntityRow();
//        dbBytePage<dbEntityRow> pageType = new dbBytePage<>(options.pageSize,entityType);
//        dbEntityLoader<dbEntityRow,dbBytePage<dbEntityRow>> loader = new dbEntityLoader<>(options.file, pageType, entityType);
    }
}
