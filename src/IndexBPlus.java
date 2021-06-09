import IndexKeyValues.IntegerKey;
import IndexKeyValues.Value;
import ItemSerializer.BaseItemSerializer;
import Loaders.BaseItemLoader;
import Overall.RowOfData;
import Tree.Root;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class IndexBPlus {
    private static String input_file;
    private static int input_PageSize;

    public static void main(String[] args) {
        if (args.length != (constants.DBLOAD_ARG_COUNT-1)) {
            System.out.println("Error: Incorrect number of arguments were input");
            return;
        }

            input_file = "./OutFiles/"+args[1]+".heap";
            input_PageSize = Integer.parseInt(args[1]);

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
        RowOfData row = new RowOfData();
        BaseItemSerializer<RowOfData> page = new BaseItemSerializer<>(page_Size,row);
        BaseItemLoader<RowOfData,BaseItemSerializer<RowOfData>> loader = new BaseItemLoader<>(file, page, row);

        try{
            loader.connect();
            System.out.println("Creating Indexes in B+ Tree");
            long indexBuildStart = System.currentTimeMillis();
            Iterator<RowOfData> rows = loader.iterator();
            while(rows.hasNext()){
                RowOfData tmpRow = rows.next();
                IntegerKey tmpKey = new IntegerKey(tmpRow.id);
                Value tmpLocation = new Value(tmpRow.getKey());
                rootNode.insert(tmpKey, tmpLocation);
            }
            long indexBuildEnd = System.currentTimeMillis();
            System.out.println("Total time: " + (indexBuildEnd - indexBuildStart) + "ms");
        }catch(Exception e) {
            e.printStackTrace();
        }
        try {
            int optionChosen;
            int searchIdequality, searchIdRangeLower, searchIdRangeUpper;
            long indexStart, indexEnd, indexTotal,
                    heapStart, heapEnd, heapTotal = -1;
            while(true){

                optionChosen = readInteger("Enter 1 for Equality Search, 2 For Range Search :");
                if(!(optionChosen==1 ||optionChosen ==2))
                {
                    System.out.println("That is not a valid input");
                    continue;
                }
                else if (optionChosen ==1)
                {
                    searchIdequality = readInteger("Query Id Number :");
                    indexStart = System.currentTimeMillis();
                    Value result = rootNode.search(
                            new IntegerKey(searchIdequality)
                    );
                    indexEnd = System.currentTimeMillis();
                    indexTotal = (indexEnd - indexStart);
                    System.out.println(String.format("Index Search Time in B+ Tree: %d", (indexTotal)));
                    if(result == null){
                        System.out.println("No record found");
                        continue;
                    }

                    heapStart = System.currentTimeMillis();
                    RowOfData searchRow = loader.findId(result.getKey());
                    heapEnd = System.currentTimeMillis();
                    heapTotal = (heapEnd - heapStart);
                    System.out.println(String.format("Heap Query Time after creating Indexes: %d", (heapTotal)));

                    System.out.println(String.format("Total Query Time:%d", (heapTotal + indexTotal)));


                    System.out.println(result);
                    System.out.println(searchRow.toString());
                }else if(optionChosen ==2)
                {
                    searchIdRangeUpper=0;
                    searchIdRangeLower = readInteger("Enter the lower Id Number :");
                    boolean valid = false;
                    while(!valid)
                    {
                        searchIdRangeUpper = readInteger("Enter the upper Id Number :");
                        if(searchIdRangeUpper>searchIdRangeLower)
                            valid =true;
                        else
                            System.out.println("Please enter a number higher than the Lower number");
                    }

                    indexStart = System.currentTimeMillis();
                    heapTotal=0;
                    for(int i = searchIdRangeLower; i<=searchIdRangeUpper ;i++)
                    {
                        Value result = rootNode.search(
                                new IntegerKey(i)
                        );
                        if(result == null){
                            System.out.println("No record found");
                            continue;
                        }
                        heapStart = System.currentTimeMillis();
                        RowOfData searchRow = loader.findId(result.getKey());
                        heapEnd = System.currentTimeMillis();
                        heapTotal += (heapEnd - heapStart);
                        System.out.println(result);
                        System.out.println(searchRow.toString());
                    }
                    indexEnd = System.currentTimeMillis();
                    indexTotal = (indexEnd - indexStart);
                    System.out.println(String.format("Total Time in B+ Tree: %d", (indexTotal)));
                    System.out.println(String.format("Heap Query Time after creating Indexes: %d", (heapTotal)));
                    System.out.println(String.format("Total Query Time:%d", (heapTotal + indexTotal)));
                }
            }

        //dbEntityRow entityType = new dbEntityRow();
       // dbBytePage<dbEntityRow> pageType = new dbBytePage<>(options.pageSize,entityType);
        //dbEntityLoader<dbEntityRow,dbBytePage<dbEntityRow>> loader = new dbEntityLoader<>(options.file, pageType, entityType);
    } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Integer readInteger(String message){
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while(true){
            String input = "";
            try{
                System.out.println(message);
                input = reader.readLine();
                if(input == "exit"){
                    System.out.println("Exiting");
                    System.exit(0);
                }
                int output = Integer.parseInt(input);
                return output;
            }catch(Exception e){
                System.out.println("Could not cast "+ input+" to integer please enter an integer or (exit) to exit.");
            }
        }
    }
}
