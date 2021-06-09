import java.io.BufferedReader;
import java.io.FileReader;

import BaseClasses.Key;
import ItemSerializer.BaseItemSerializer;
import Loaders.BaseItemLoader;
import Overall.RowOfData;


public class dbload {
    public static final String HEAP_FNAME = "./data/heap.";

    public static void main(String args[])
    {
        if (args.length != constants.DBLOAD_ARG_COUNT) {
            System.out.println("Error: Incorrect number of arguments were input");
            return;
        }

            dbload dbloadMain = new dbload();
            try{

                long startTime = System.currentTimeMillis();
                dbloadMain.run(args[2],Integer.parseInt(args[1]));
                long endTime = System.currentTimeMillis();

                System.out.println("dbload time: " + (endTime - startTime) + "ms");


            }catch ( Exception e ){
                System.out.println("Error In Main");
                e.printStackTrace();
            }
    }




    public void run(String file, int page_Size){
        RowOfData data = new RowOfData();
        Key key = new Key();
        BaseItemSerializer<RowOfData> page = new BaseItemSerializer<RowOfData>(page_Size, data);
        BaseItemLoader<RowOfData, BaseItemSerializer<RowOfData>> client = new BaseItemLoader<RowOfData, BaseItemSerializer<RowOfData>>("./OutFiles/"+page_Size+".heap", page, data);
        BufferedReader br = null;
        String line;
        String  filename = file;
        String stringDelimeter = ",";

        try{
            client.connect();

            br = new BufferedReader(new FileReader(filename));
            line = br.readLine();
            while ((line = br.readLine()) != null)
            {
                String[] entry = line.split(stringDelimeter, -1);
                RowOfData tmp = data.initialize(entry, key);
                client.insertEntity(tmp);
            }
            client.close();

        }catch(Exception e){
            e.printStackTrace();
        }finally{
        }


    }

    public static boolean verifyArgs(String[] args)
    {
        if(args[1].compareTo("-p")==0)
        {
            try{
                Integer.parseInt(args[2]);
                return true;
            }catch (Exception e)
            {
                return false;
            }

        }
        return false;
    }
}
