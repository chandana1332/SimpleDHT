package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by chandana on 2/18/15.
 */
public class DatabaseOps extends SQLiteOpenHelper {

    public static final int database_version = 3;
    public String Create = "CREATE TABLE " + DataTable.TableInfo.TABLE_NAME + "(" + DataTable.TableInfo.key + " TEXT," + DataTable.TableInfo.value + " TEXT,PRIMARY KEY (" + DataTable.TableInfo.key + "));";

    public DatabaseOps(Context context) {
        super(context, DataTable.TableInfo.DATABASE_NAME, null, database_version);

    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(Create);
        Log.d("Database Ops", "Table created");
    }

    public void insertinto(DatabaseOps dbo, ContentValues values) {

        SQLiteDatabase sdb = dbo.getWritableDatabase();

        long k = sdb.insert(DataTable.TableInfo.TABLE_NAME, null, values);

        if (k < 0) {

            String q1 = "UPDATE " + DataTable.TableInfo.TABLE_NAME + " SET value=\"" + values.get(DataTable.TableInfo.value) + "\" where key=\"" + values.get(DataTable.TableInfo.key) + "\"";
            Log.d("Update", "Update successful");

        }
        Log.d("Database Ops", "Data inserted");
        displaydata(dbo);

    }

    public Cursor displaydata(DatabaseOps dob) {
        SQLiteDatabase db = dob.getReadableDatabase();

        String Cols[] = {DataTable.TableInfo.key, DataTable.TableInfo.value};
        Cursor CR = db.rawQuery("Select * from " + DataTable.TableInfo.TABLE_NAME, null);
        CR.moveToFirst();
        while (CR.isAfterLast() == false) {
            String a = CR.getString(0);
            String b = CR.getString(1);
            System.out.println(a + "     " + b);
            CR.moveToNext();
        }
        return CR;
    }

    public void delete(DatabaseOps d) {

        SQLiteDatabase sdb = d.getWritableDatabase();
        sdb.delete(DataTable.TableInfo.TABLE_NAME, null, null);
    }

    public Cursor retrievefrom(DatabaseOps dob, String key) {

        SQLiteDatabase db = dob.getReadableDatabase();
        String Cols[] = {DataTable.TableInfo.key, DataTable.TableInfo.value};
        String wherearg = "key=" + key;
        Cursor CR = db.query(DataTable.TableInfo.TABLE_NAME, Cols, DataTable.TableInfo.key + "='" + key + "'", null, null, null, null);
        if (CR != null) {
            CR.moveToFirst();
        }
        return CR;
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}
