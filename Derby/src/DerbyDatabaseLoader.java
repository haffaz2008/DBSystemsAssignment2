import model.Counter;
import model.DateTime;
import model.Sensor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DerbyDatabaseLoader {
    private String[] months;
    private String[] days;
    private List<Counter> counterList;
    private List<DateTime> dateTimeList;
    private Set<Integer> dateIdSet;
    private List<Sensor> sensorList;
    private Set<Integer> sensorIdSet;
    private String dateTable;
    private String sensorTable;
    private String countTable;
    private String[] tables;
    private String[][] sqlList;

    /**
     * convert the month or day into respective number, "January" -> 1, "Wednesday" -> 3
     *
     * @param key String description for month or day, e.g. "January", "Wednesday"
     * @param map String array of months or days
     * @return the respective number for the month or day
     */
    private static int getInt(String key, String[] map) {
        int len = map.length;
        // ignore index 0
        for (int i = 1; i < len; i++) {
            if (map[i].equalsIgnoreCase(key)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * convert String to number
     *
     * @param str to be converted String
     * @return the number after conversion
     */
    private static int toInt(String str) {
        return Integer.parseInt(str);
    }

    /**
     * Drop table if exists
     * important code reference:
     * http://somesimplethings.blogspot.com/2010/03/derby-create-table-if-not-exists.html
     *
     * @param table table name
     * @param conn  connection to the Database
     * @param state statement
     * @throws SQLException exception when invalid sql
     */
    private static void dropTable(String table, Connection conn, Statement state) throws SQLException {
        DatabaseMetaData databaseMetadata = conn.getMetaData();
        ResultSet resultSet;
        resultSet = databaseMetadata.getTables(null, null, table, null);
        // use table name to retrieve table metadata, if has next then indicates table exists
        if (resultSet.next()) {
            String sql = String.format("drop table %s", table);
            state.execute(sql);
            // use std_out as mentioned in the requirement
            System.out.println("Dropped table: " + table);
        }
    }

    /**
     * DerbyDatabaseLoader Database loading program driver function, program entry point
     *
     * @param args command line arguments - an array of String arguments
     */
    public static void main(String[] args) {
        DerbyDatabaseLoader derbyDatabaseLoader = new DerbyDatabaseLoader();
        if (!derbyDatabaseLoader.verifyArgs(args)) {
            return;
        }
        final String path = args[0];
        System.out.println("DerbyDatabaseLoader begun");
        derbyDatabaseLoader.init();
        derbyDatabaseLoader.loadDate(path);
        System.out.println("DerbyDatabaseLoader program complete");
    }

    /**
     * Verify if the command line arguments meet the requirement
     * standard format: java DerbyDatabaseLoader datafile
     * args-1: path for datafile
     *
     * @param args an array of String arguments
     * @return true if requirements met, false otherwise
     */
    private boolean verifyArgs(String[] args) {
        int required = 1;
        // if 1 condition met, a) no file path provided; b) file does not exist
        if (args == null || args.length < required || !(new File(args[0]).exists())) {
            System.err.println("insufficient number of arguments OR invalid arguments OR CSV file not exist");
            System.err.println("command to execute the program: java DerbyDatabaseLoader datafile");
            System.err.println("example: java DerbyDatabaseLoader file.csv");
            return false;
        }
        return true;
    }

    /**
     * important - code reference:
     * https://www.guru99.com/buffered-reader-in-java.html
     * Load data from csv file into memory, structure the data into 3 entities:
     * 1. count entity
     * 2. date time entity
     * 3. sensor entity
     *
     * @param path the directory path leads to the source file
     */
    private void loadDate(String path) {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            final String comma = ",";
            boolean isHeader = true;
            String record;
            String[] data;
            int countId, dateId, year, month, date, day, time, sensorId, hourlyCounts;
            String dateDesc, yearStr, monthStr, dateStr, dayStr, timeStr, sensorName;
            int countIdIdx = 0, dateTimeIdx = 1, yearIdx = 2, monthIdx = 3, dateIdx = 4;
            int dayIdx = 5, timeIdx = 6, sensorIdIdx = 7, sensorNameIdx = 8, hourlyCountsIdx = 9;

            while ((record = br.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                data = record.split(comma);
                countId = toInt(data[countIdIdx].trim());
                dateDesc = data[dateTimeIdx].trim();
                yearStr = data[yearIdx].trim();
                monthStr = data[monthIdx].trim();
                dateStr = data[dateIdx].trim();
                dayStr = data[dayIdx].trim();
                timeStr = data[timeIdx].trim();
                sensorId = toInt(data[sensorIdIdx].trim());
                sensorName = data[sensorNameIdx].trim();
                hourlyCounts = toInt(data[hourlyCountsIdx].trim());
                year = toInt(yearStr);
                date = toInt(dateStr);
                time = toInt(timeStr);
                month = getInt(monthStr, months);
                day = getInt(dayStr, days);
                dateId = toInt(yearStr + (month < 10 ? "0" + month : month)
                        + (date < 10 ? "0" + date : date) + (time < 10 ? "0" + time : time));
                addToDateList(dateId, dateDesc, year, month, date, day, time);
                addToSensorList(sensorId, sensorName);
                addToCountList(countId, hourlyCounts, dateId, sensorId);
            }
            dateTimeList.sort((o1, o2) -> o1.getId() - o2.getId());
            sensorList.sort((o1, o2) -> o1.getId() - o2.getId());
            counterList.sort((o1, o2) -> o1.getId() - o2.getId());
            // use std_out as required
            System.out.println("Data Load Complete");
            importToDerby();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Insert all the dates from date list to DerbyDatabaseLoader DB
     *
     * @param psInsert prepared statement for insertion
     * @throws SQLException SQL exception
     */
    private void insertDates(PreparedStatement psInsert) throws SQLException {
        for (DateTime date : dateTimeList) {
            psInsert.setInt(1, date.getId());
            psInsert.setString(2, date.getDesc());
            psInsert.setInt(3, date.getYear());
            psInsert.setInt(4, date.getMonth());
            psInsert.setInt(5, date.getDate());
            psInsert.setInt(6, date.getDay());
            psInsert.setInt(7, date.getTime());
            psInsert.executeUpdate();
        }
    }

    /**
     * Insert all the sensors from sensor list to DerbyDatabaseLoader DB
     *insertSensors(PreparedStatement psInsert) throws SQLException {
        for (Sensor sensor : sensorList) {
            psInsert.setInt(1, sensor.getId());
            psInsert.setString(2, sensor.getName());
            psInsert.executeUpdate();
        }
    }

    /**
     * Insert all the counts from counts list to DerbyDatabaseLoader DB
     *
     * @param psInsert prepared statement for insertion
     * @throws SQLException SQL exception
     */
    private void insertCounts(PreparedStatement psInsert) throws SQLException {
        for (Counter counter : counterList) {
            psInsert.setInt(1, counter.getId());
            psInsert.setInt(2, counter.getHourlyCount());
            psInsert.setInt(3, counter.getDateTimeId());
            psInsert.setInt(4, counter.getSensorId());
            psInsert.executeUpdate();
        }
    }

    /**
     * Add all the datetime records from csv file to date list
     *
     * @param dateId   date id
     * @param dateDesc date time description
     * @param year     year int
     * @param month    month int
     * @param date     date int
     * @param day      day int
     * @param time     time int
     */
    private void addToDateList(int dateId, String dateDesc, int year, int month, int date, int day, int time) {
        if (!dateIdSet.contains(dateId)) {
            dateIdSet.add(dateId);
            dateTimeList.add(new DateTime(dateId, dateDesc, year, month, date, day, time));
        }
    }

    /**
     * Add all the hourly counts records from csv file to counts list
     *
     * @param countId  count id
     * @param counts   hourly counts
     * @param dateId   datetime id
     * @param sensorId sensor id
     */
    private void addToCountList(int countId, int counts, int dateId, int sensorId) {
        counterList.add(new Counter(countId, counts, dateId, sensorId));
    }

    /**
     * Add all the sensors from csv file to sensor list
     *
     * @param sensorId   sensor id
     * @param sensorName sensor name
     */
    private void addToSensorList(int sensorId, String sensorName) {
        if (!sensorIdSet.contains(sensorId)) {
            sensorIdSet.add(sensorId);
            sensorList.add(new Sensor(sensorId, sensorName));
        }
    }

    /**
     * create 3 tables and insert all the data to the Database
     *
     * @param conn  DB connection
     * @param state statement
     * @param table table name
     * @param sql   SQL schema - create, insert, select
     */
    private void createAndInsert(Connection conn, Statement state, String table, String[] sql) {
        String createSql = sql[0];
        String insertSql = sql[1];
        String querySql = sql[2];

        ResultSet result;
        System.out.println("Sub task: creating table " + table);
        try {
            state.execute(createSql);
            PreparedStatement psInsert = conn.prepareStatement(insertSql);
            System.out.printf("Sub task: insert records into %s%n", table);
            if (table.equals(sensorTable)) {
                insertSensors(psInsert);
            } else if (table.equals(countTable)) {
                insertCounts(psInsert);
            } else if (table.equals(dateTable)) {
                insertDates(psInsert);
            }
            // Release the resources
            if (psInsert != null) {
                psInsert.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * important code reference:
     * https://stackoverflow.com/questions/18593019/if-exists-not-recognized-in-derby
     * partial code copied and modified from DerbyDatabaseLoader installation directory Sample program WwdEmbedded.java
     * under directory: ./derby/demo/programs/workingwithderby/WwdEmbedded.java
     */
    private void importToDerby() {
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        String protocol = "jdbc:derby:";
        String dbName = "DerbyDB";
        String connectionUrl = protocol + dbName + ";create=true";

        int number = tables.length;
        String table;
        String[] sql;
        try (
                // auto closable, no need to include finally block to release resources
                Connection conn = DriverManager.getConnection(connectionUrl);
                Statement state = conn.createStatement()
        ) {
            // use std_out as required
            System.out.println("Connected to database " + dbName);
            // control transactions manually, autocommit is on by default in JDBC
            conn.setAutoCommit(false);
            // drop the table if exists
            for (int i = number - 1; i >= 0; --i) {
                table = tables[i];
                dropTable(table, conn, state);
            }
            // use std_out as required
            System.out.println("Loading data from memory into DerbyDatabaseLoader");
            long start = System.currentTimeMillis();
            for (int i = 0; i < number; i++) {
                table = tables[i];
                sql = sqlList[i];
                createAndInsert(conn, state, table, sql);
            }
            long end = System.currentTimeMillis();
            // calculate the total time taken for loading data into DerbyDatabaseLoader DB
            long duration = end - start;
            // use std_out as required
            System.out.printf("Time Taken to complete DatabaseLoad %d millisecond = %.2f seconds%n", duration, duration / 1000f);
            //  commit the transaction: any changes will be persisted to the database now
            conn.commit();
            // use std_out as required
            System.out.println("Committed the transaction");
            System.out.println("Closed connection");
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
            //  Catch all exceptions and pass them to
            //  the Throwable.printStackTrace method
            System.err.println(" . . . exception thrown:");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * initialise instance variables for DerbyDatabaseLoader database creation and insertion
     * initialise list containers to load data from CSV file to memory container
     */
    private void init() {
        // initialise months and days array for later conversion, e.g. "January" -> 1, "Monday" -> 1
        // index 0 will be ignored, conversion starts from index 1
        months = new String[]{"", "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"};
        days = new String[]{"", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

        sensorTable = "SENSOR";
        String[] sensorSql = new String[]{
                String.format("create table %s (id int not null, name varchar(40) not null, primary key (id))", sensorTable),
                String.format("insert into %s (id, name) values (?, ?)", sensorTable),
                String.format("select id, name from %s", sensorTable)
        };

        dateTable = "DATETIME";
        String[] dateSql = new String[]{
                "create table " + dateTable + " (id int not null, desc_str varchar(24) not null,"
                        + " year_int int not null, month_int int not null, date_int int not null,"
                        + " day_int int not null, time_int int not null, primary key (id))",
                String.format("insert into %s (id, desc_str, year_int, month_int, date_int, day_int, time_int) values (?, ?, ?, ?, ?, ?, ?)", dateTable),
                String.format("select id, desc_str, year_int, month_int, date_int, day_int, time_int from %s", dateTable)
        };

        countTable = "COUNT";
        // for count table, apply foreign key constraints reference from sensor and datetime table
        String[] countSql = new String[]{
                String.format("create table %s (id int not null, counts int not null, dateId int not null,"
                        + "sensorId int not null, primary key (id), foreign key (dateId) references %s (id),"
                        + "foreign key (sensorId) references %s (id))", countTable, dateTable, sensorTable),
                String.format("insert into %s (id, counts, dateId, sensorId) values (?, ?, ?, ?)", countTable),
                String.format("select id, counts, dateId, sensorId from %s", countTable)
        };
        tables = new String[]{dateTable, sensorTable, countTable};
        sqlList = new String[][]{dateSql, sensorSql, countSql};

        // initialise the list and set containers
        // usage for set is to eliminate data duplicates
        counterList = new ArrayList<>();
        dateTimeList = new ArrayList<>();
        dateIdSet = new HashSet<>();
        sensorList = new ArrayList<>();
        sensorIdSet = new HashSet<>();
    }
    /**
     * Insert all the sensors from sensor list to DerbyDatabaseLoader DB
     *
     * @param psInsert prepared statement for insertion
     * @throws SQLException SQL exception
     */
    private void insertSensors(PreparedStatement psInsert) throws SQLException {
        for (Sensor sensor : sensorList) {
            psInsert.setInt(1, sensor.getId());
            psInsert.setString(2, sensor.getName());
            psInsert.executeUpdate();
        }
    }
}