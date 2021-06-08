package MainRunners;

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
        RowOfData row = new RowOfData();
        BaseItemSerializer<RowOfData> page = new BaseItemSerializer<>(page_Size,row);
        BaseItemLoader<RowOfData,BaseItemSerializer<RowOfData>> loader = new BaseItemLoader<>(file, page, row);

        try{
            loader.connect();
            System.out.println("Loading Index");
            long indexBuildStart = System.currentTimeMillis();
            Iterator<RowOfData> rows = loader.iterator();
            while(rows.hasNext()){
                RowOfData tmpRow = rows.next();
                IntegerKey tmpKey = new IntegerKey(tmpRow.id);
                Value tmpLocation = new Value(tmpRow.getKey());
                rootNode.insert(tmpKey, tmpLocation);
            }
            long indexBuildEnd = System.currentTimeMillis();
            System.out.println("Index Build time: " + (indexBuildEnd - indexBuildStart) + "ms");
        }catch(Exception e) {
            e.printStackTrace();
        }
        try {
            int searchId;
            long indexStart, indexEnd, indexTotal,
                    heapStart, heapEnd, heapTotal = -1;
            while(true){
                searchId = readInteger("Enter id:");
                System.out.println("Retrieving ID "+ searchId);

                indexStart = System.currentTimeMillis();
                Value result = rootNode.search(
                        new IntegerKey(searchId)
                );
                indexEnd = System.currentTimeMillis();
                indexTotal = (indexEnd - indexStart);
                System.out.println(String.format("Index Query Time: %d", (indexTotal)));
                if(result == null){
                    System.out.println("No record found");
                    continue;
                }

                heapStart = System.currentTimeMillis();
                RowOfData searchRow = loader.findEntity(result.getKey());
                heapEnd = System.currentTimeMillis();
                heapTotal = (heapEnd - heapStart);
                System.out.println(String.format("Heap Query Time: %d", (heapTotal)));

                System.out.println(String.format("Total Query Time:%d", (heapTotal + indexTotal)));


                System.out.println(result);
                System.out.println(searchRow.toString());

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
