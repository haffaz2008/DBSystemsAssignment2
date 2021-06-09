import java.sql.*;


public class DerbyIndexAdd {

    public static void main(String[] args) {
        DerbyIndexAdd derby = new DerbyIndexAdd();

        int input = derby.verifyArgs(args);
        if (input == 0) {
            return;
        }
        final int queryNumber = input;
        System.out.println("Index Adder program starts");

        derby.loadDate(queryNumber);
        System.out.println("Index Adder finished");
    }

    int verifyArgs(String[] args) {
        int required = 1;
        try{
            Integer.parseInt(args[0]);
            int input = Integer.parseInt(args[0]);

            if (input<1 || input > 4)
            {

                System.err.println("Enter Valid Number");
                return 0;
            }
            else
            {
                return input;
            }

        }catch(Exception e)
        {
            System.err.println("Not a valid expression");
            return 0;
        }

    }

    void loadDate(int query) {

        executeQuery(query);
    }


    private void queryRunner(Connection conn, Statement state, String queryUsed) {

        System.out.println("Creating Index ");
        int counter =0;
        try {
            PreparedStatement statement = conn.prepareStatement(queryUsed);
            statement.execute();

//            while(result.next()){
//                counter ++;
//            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        System.out.println("Number of Results found =  "+counter);

    }

    private void executeQuery(int query) {
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        String protocol = "jdbc:derby:";
        String dbName = "DerbyDB";
        String connectionUrl = protocol + dbName + ";create=true";
        String query1 = "create index count_id_index on count(id);";
        String query2 = "create index count_date_id_index on count(counts,dateId);";
        String query3 = "create index count_id_index on count(dateId)";
        String query4 = "create index count_id_index on count(sensorId)";
        String queryUsed= "";

        switch(query)
        {
            case 1 : queryUsed=query1;break;
            case 2 : queryUsed = query2;break;
            case 3 : queryUsed = query3;break;
            case 4 : queryUsed = query4;break;
            default:
                System.out.println("Invalid");break;
        }
        try (

                Connection conn = DriverManager.getConnection(connectionUrl);
                Statement state = conn.createStatement()
        ) {

            System.out.println("Connected to database " + dbName);
            conn.setAutoCommit(false);
            System.out.println(" Querying Data Now");
            long start = System.currentTimeMillis();

            queryRunner(conn, state, queryUsed);

            long end = System.currentTimeMillis();
            long duration = end - start;
            System.out.printf("Time taken to complete Addition \n %d millisecond = %.2f seconds%n", duration, duration / 1000f);
            conn.commit();
            System.out.println("Transaction Committed");
            System.out.println("Connection closed");
            boolean gotSqlExc = false;
            try {
                DriverManager.getConnection(protocol + ";shutdown=true");
            } catch (SQLException se) {
                String ex = "XJ015";
                if (ex.equals(se.getSQLState())) {
                    gotSqlExc = true;
                }
            }
            if (!gotSqlExc) {
                System.err.println("Database did not shut down normally");
            } else {
                System.err.println("Database shut down normally");
            }
        } catch (Throwable e) {
            System.err.println("exception thrown:");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }


}