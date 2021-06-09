package Overall;

import BaseClasses.*;

import java.io.UnsupportedEncodingException;



public class RowOfData extends BaseItem<RowOfData> {

    //#region Static Sizes and Offsets
    /* Fixed Variable Lengths */

    /* Sizes */
    public static final int ID_SIZE = 4;
    public static final int DATETIME_SIZE = 22;
    public static final int YEAR_SIZE = 4;
    public static final int MONTH_SIZE = 9;
    public static final int DAY_SIZE = 9;
    public static final int MDATE_SIZE = 4;
    public static final int TIME_SIZE = 4;
    public static final int SENSORID_SIZE = 4;
    public static final int SENSORNAME_SIZE = 38;
    public static final int HOURLYCOUNT_SIZE = 4;

    public static final int RECORD_SIZE =  Key.CONTENT_SIZE
            + ID_SIZE
            + DATETIME_SIZE
            + YEAR_SIZE
            + MONTH_SIZE
            + MDATE_SIZE
            + DAY_SIZE
            + TIME_SIZE
            + SENSORID_SIZE
            + SENSORNAME_SIZE
            + HOURLYCOUNT_SIZE;

    /* Columns */
    public static final int ID_COL = 0;
    public static final int DATE_COL = 1;
    public static final int YEAR_COL = 2;
    public static final int MONTH_COL = 3;
    public static final int MDATE_COL = 4;
    public static final int DAY_COL = 5;
    public static final int TIME_COL = 6;
    public static final int SENSORID_COL = 7;
    public static final int SENSORNAME_COL = 8;
    public static final int HOURLYCOUNT_COL = 9;


    /* Offsets */
    public static final int DBKEY_OFFSET = 0;
    public static final int ID_OFFSET = DBKEY_OFFSET + Key.CONTENT_SIZE;
    public static final int DATETIME_OFFSET = ID_OFFSET + ID_SIZE;
    public static final int YEAR_OFFSET = DATETIME_OFFSET + DATETIME_SIZE;
    public static final int MONTH_OFFSET = YEAR_OFFSET + YEAR_SIZE;
    public static final int MDATE_OFFSET = MONTH_OFFSET + MONTH_SIZE;
    public static final int DAY_OFFSET = MDATE_OFFSET + MDATE_SIZE;
    public static final int TIME_OFFSET = DAY_OFFSET + DAY_SIZE;
    public static final int SENSORID_OFFSET = TIME_OFFSET + TIME_SIZE;
    public static final int SENSORNAME_OFFSET = SENSORID_OFFSET + SENSORID_SIZE;
    public static final int HOURLYCOUNT_OFFSET = SENSORNAME_OFFSET + SENSORNAME_SIZE;

    //#endregion


    public int id;
    public String date;
    public int year;
    public String month;
    public int mDate;
    public String day;
    public int time;
    public int sensorId;
    public String sensorName;
    public int hourlyCount;

    public RowOfData(){}



    public RowOfData initialize(String[] in, Key key)
            throws UnsupportedEncodingException
    {
        byte[] record = new byte[this.getSize()];
        Serialize.bytes(  key.serialize(),      Key.CONTENT_SIZE , DBKEY_OFFSET       , record);
        Serialize.integer(    in[ID_COL],           ID_SIZE            , ID_OFFSET          , record);
        Serialize.string( in[DATE_COL],         DATETIME_SIZE      , DATETIME_OFFSET    , record);
        Serialize.integer(    in[YEAR_COL],         YEAR_SIZE          , YEAR_OFFSET        , record);
        Serialize.string( in[MONTH_COL],        MONTH_SIZE         , MONTH_OFFSET       , record);
        Serialize.integer(    in[MDATE_COL],        MDATE_SIZE         , MDATE_OFFSET       , record);
        Serialize.string( in[DAY_COL],          DAY_SIZE           , DAY_OFFSET         , record);
        Serialize.integer(    in[TIME_COL],         TIME_SIZE          , TIME_OFFSET        , record);
        Serialize.integer(    in[SENSORID_COL],     SENSORID_SIZE      , SENSORID_OFFSET    , record);
        Serialize.string( in[SENSORNAME_COL],   SENSORNAME_SIZE    , SENSORNAME_OFFSET  , record);
        Serialize.integer(    in[HOURLYCOUNT_COL],  HOURLYCOUNT_SIZE   , HOURLYCOUNT_OFFSET , record);
        return this.deserialize(record);
    }

    @Override
    public byte[] serialize()
            throws UnsupportedEncodingException
    {
        byte[] record = new byte[this.getSize()];
        Serialize.bytes(  this.key.serialize(),  Key.CONTENT_SIZE   , DBKEY_OFFSET       , record);
        Serialize.integer(    this.id,               ID_SIZE             , ID_OFFSET          , record);
        Serialize.string( this.date,             DATETIME_SIZE       , DATETIME_OFFSET    , record);
        Serialize.integer(    this.year,             YEAR_SIZE           , YEAR_OFFSET        , record);
        Serialize.string( this.month,            MONTH_SIZE          , MONTH_OFFSET       , record);
        Serialize.integer(    this.mDate,            MDATE_SIZE          , MDATE_OFFSET       , record);
        Serialize.string( this.day,              DAY_SIZE            , DAY_OFFSET         , record);
        Serialize.integer(    this.time,             TIME_SIZE           , TIME_OFFSET        , record);
        Serialize.integer(    this.sensorId,         SENSORID_SIZE       , SENSORID_OFFSET    , record);
        Serialize.string( this.sensorName,       SENSORNAME_SIZE     , SENSORNAME_OFFSET  , record);
        Serialize.integer(    this.hourlyCount,      HOURLYCOUNT_SIZE    , HOURLYCOUNT_OFFSET , record);
        return record;
    }

    @Override
    public RowOfData deserialize(byte[] record)
            throws UnsupportedEncodingException
    {
        RowOfData dto   = new RowOfData();
        Key key      = new Key();
        dto.key        = key.deserialize(
                Deserialize.bytes(  record, Key.CONTENT_SIZE    ,  DBKEY_OFFSET     ));
        dto.id         = Deserialize.integer(    record, ID_SIZE          ,  ID_OFFSET         );
        //System.out.println("deserialized.id "+dto.id);
        dto.date       = Deserialize.string( record, DATETIME_SIZE    ,  DATETIME_OFFSET   );
        //System.out.println("deserialized.date "+dto.date);
        dto.year       = Deserialize.integer(    record, YEAR_SIZE        ,  YEAR_OFFSET       );
        //System.out.println("deserialized.year "+dto.year);
        dto.month      = Deserialize.string( record, MONTH_SIZE       ,  MONTH_OFFSET      );
        //System.out.println("deserialized.month "+dto.month);
        dto.mDate      = Deserialize.integer(    record, MDATE_SIZE       ,  MDATE_OFFSET      );
        //System.out.println("deserialized.mDate "+dto.mDate);
        dto.day        = Deserialize.string( record, DAY_SIZE         ,  DAY_OFFSET        );
        //System.out.println("deserialized.day "+dto.day);
        dto.time       = Deserialize.integer(    record, TIME_SIZE        ,  TIME_OFFSET       );
        //System.out.println("deserialized.time "+dto.time);
        dto.sensorId   = Deserialize.integer(    record, SENSORID_SIZE    ,  SENSORID_OFFSET   );
        //System.out.println("deserialized.sensorId "+dto.sensorId);
        dto.sensorName = Deserialize.string( record, SENSORNAME_SIZE  ,  SENSORNAME_OFFSET );
        //System.out.println("deserialized.sensorName "+dto.sensorName);
        dto.hourlyCount= Deserialize.integer(    record, HOURLYCOUNT_SIZE ,  HOURLYCOUNT_OFFSET);
        //System.out.println("deserialized.hourlyCount "+dto.hourlyCount);
        return dto;
    }



    @Override
    public int getSize() {
        return RECORD_SIZE;
    }

    @Override
    public String toString() {
        return String.format( "\npageid\": %d, \"rid\": %d, \"size\": %d, \"id\": %d, \"date\": \"%s\", \"year\": %d, \"month\": \"%s\", \"mDate\": %d, \"day\": \"%s\", \"time\": %d, \"sensorId\": %d, \"sensorName\": \"%s\", \"hourlyCount\": %d }",
                this.key.getPageId()
                ,this.key.getRId()
                ,this.getSize()
                ,this.id
                ,this.date
                ,this.year
                ,this.month
                ,this.mDate
                ,this.day
                ,this.time
                ,this.sensorId
                ,this.sensorName
                ,this.hourlyCount
        );
    }

}