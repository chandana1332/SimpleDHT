package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    static final String[] ports = new String[]{"11108", "11112", "11116", "11120", "11124"};
    static String first_avd = "11108";
    String sfirst_en = "";
    static String myPort = "";
    public String first_en;
    static final int SERVER_PORT = 10000;
    static final String TAG = "DHT:";
    public String succ = null;
    public String pre = null;
    String slf;
    int scount = 0;
    String result = "";
    boolean starflag = false;
    public static int count = 0;
    String fflag = "";
    public static ArrayList<String> ring;
    public static HashMap<String, String> hashmatch = new HashMap<String, String>();
    Uri mUri = buildUri("content",
            "edu.buffalo.cse.cse486586.simpledht.provider");
    public static boolean flag;
    public static boolean ch = false;
    public static Cursor cr = null;
    boolean flag1;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Context c = getContext();
        DatabaseOps dbo = new DatabaseOps(c);
        dbo.delete(dbo);
        Cursor t = dbo.displaydata(dbo);

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    //---------INSERT-----------------------------------
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Context c = getContext();

        DatabaseOps dob = new DatabaseOps(c);

        try {
            String key_hash = genHash(values.get("key").toString());

            if (succ == null) {
                dob.insertinto(dob, values);
            } else {
                if (genHash(myp).compareTo(pre) < 0) {
                    if ((key_hash.compareTo(genHash(myp)) <= 0) || (key_hash.compareTo(pre) > 0)) {

                        dob.insertinto(dob, values);

                    } else {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT", values.get(DataTable.TableInfo.key).toString(), hashmatch.get(succ), values.get(DataTable.TableInfo.value).toString());

                    }
                } else {
                    if (key_hash.compareTo(genHash(myp)) <= 0 && (key_hash.compareTo(pre) > 0)) {
                        dob.insertinto(dob, values);
                    } else {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT", values.get(DataTable.TableInfo.key).toString(), hashmatch.get(succ), values.get(DataTable.TableInfo.value).toString());

                    }
                }
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return uri;

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        System.out.println("In on create");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        System.out.println(myPort);
        try {
            /*
             e.printStackTrace();
             }


             final EditText editText = (EditText) findViewById(R.id.editText1);

             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */

            ring = new ArrayList<String>();

            for (String p : ports) {
                hashmatch.put(genHash(String.valueOf(Integer.parseInt(p) / 2)), p);
            }

            if (myPort.equalsIgnoreCase(first_avd)) {

                ring.add(genHash(String.valueOf(Integer.parseInt(myPort) / 2)));

                Collections.sort(ring, new Comparator<String>() {
                    @Override
                    public int compare(String t1, String t2) {

                        return t1.compareTo(t2);
                    }
                });

            } else {

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JOIN", myPort, first_avd);

            }

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "In ONCREATE! Can't create a ServerSocket");
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        count = count + 1;
        cr = null;
        ch = false;

        Context c = getContext();
        DatabaseOps dbo = new DatabaseOps(c);
        if (succ == null) {
            if (selection.equalsIgnoreCase("\"*\"")) {
                cr = dbo.displaydata(dbo);
            } else if (selection.equalsIgnoreCase("\"@\"")) {
                cr = dbo.displaydata(dbo);
            } else {
                cr = dbo.retrievefrom(dbo, selection);
            }
        } else {
            if (selection.equalsIgnoreCase("\"*\"")) {
                ++scount;
                System.out.println("In star");
                Cursor cursor = dbo.displaydata(dbo);
                cursor.moveToFirst();
                while (cursor.isAfterLast() == false) {
                    String key = cursor.getString(0);
                    String value = cursor.getString(1);
                    result = result + key + ":" + value + "|";

                    cursor.moveToNext();

                }

                cursor.close();

                if (scount == 1) {
                    sfirst_en = myPort;
                }
                try {
                    ClientTaskQ cq = new ClientTaskQ();
                    int sendCount = scount;
                    cq.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERYSTAR", result, hashmatch.get(succ), sfirst_en, String.valueOf(sendCount));

                    boolean arg = cq.get(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }

            } else if (selection.equalsIgnoreCase("\"@\"")) {
                cr = dbo.displaydata(dbo);
            } else {
                try {
                    String key_hash = genHash(selection);
                    String myp = "" + (Integer.parseInt(myPort) / 2) + "";
                    if (genHash(myp).compareTo(pre) < 0) {
                        if ((key_hash.compareTo(genHash(myp)) <= 0) || (key_hash.compareTo(pre) > 0)) {
                            cr = dbo.retrievefrom(dbo, selection);
                        } else {
                            ch = true;
                        }
                    } else {
                        if (key_hash.compareTo(genHash(myp)) <= 0 && (key_hash.compareTo(pre) > 0)) {
                            cr = dbo.retrievefrom(dbo, selection);

                        } else {
                            ch = true;
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }
        if (count == 1) {
            first_en = myPort;
        } else {

        }
        if (ch) {
            try {
                ClientTaskQ cq = new ClientTaskQ();
                int sendCount = count;
                count = 0;
                cq.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERYPASS", selection, hashmatch.get(succ), first_en, String.valueOf(sendCount));

                boolean arg = cq.get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
        Log.v("query", selection);
        if (cr != null) {
            cr.moveToFirst();
        }
        count = 0;
        scount = 0;
        return cr;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

//--------------------------SERVER CODE------------------------------------
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                Socket s = serverSocket.accept();
                String message = "";
                String messageType = "";
                String mport = "";
                String add = "";
                InputStreamReader ireader = new InputStreamReader(s.getInputStream());
                BufferedReader breader = new BufferedReader(ireader);
                messageType = breader.readLine();
                message = breader.readLine();
                mport = breader.readLine();
                add = breader.readLine();
                if (messageType.equalsIgnoreCase("JOIN")) {
                    ring.add(genHash(String.valueOf(Integer.parseInt(message) / 2)));
                    Collections.sort(ring, new Comparator<String>() {
                        @Override
                        public int compare(String t1, String t2) {

                            return t1.compareTo(t2);
                        }
                    });

                    if (ring.size() > 1) {
                        for (int i = 0; i < ring.size(); i++) {
                            if (i == 0) {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "SET PRE", ring.get(ring.size() - 1), hashmatch.get(ring.get(0)), ring.get(1));
                            } else if (i == ring.size() - 1) {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "SET PRE", ring.get(i - 1), hashmatch.get(ring.get(i)), ring.get(0));
                            } else {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "SET PRE", ring.get(i - 1), hashmatch.get(ring.get(i)), ring.get(i + 1));
                            }
                        }
                    }
                } else if (messageType.equalsIgnoreCase("SET PRE")) {
                    pre = message;
                    succ = add;
                } else if (messageType.equalsIgnoreCase("INSERT")) {
                    ContentValues val = new ContentValues();
                    val.put("key", message);
                    val.put("value", add);
                    insert(mUri, val);

                } else if (messageType.equalsIgnoreCase("QUERYRESPONSE")) {
                    String c = breader.readLine();
                    count = 0;
                    MatrixCursor max = new MatrixCursor(new String[]{"key", "value"});
                    String[] strArray = message.split("\\|");
                    for (String str : strArray) {
                        if (str.contains(":")) {
                            MatrixCursor.RowBuilder bg = max.newRow();
                            bg.add("key", str.split(":")[0]);
                            bg.add("value", str.split(":")[1]);
                            slf = str.split(":")[1];
                            Log.v("key", str.split(":")[0]);
                            Log.v("value", str.split(":")[1]);
                        }
                    }
                    max.moveToFirst();
                    cr = max;
                    max.close();
                    flag = false;
                } else if (messageType.equalsIgnoreCase("QUERYPASS")) {
                    String c = breader.readLine();
                    first_en = add;
                    count = Integer.parseInt(c);
                    cr = query(mUri, null, message, null, null);
                    if (cr != null) {
                        cr.moveToFirst();
                        String res = myPort + "|";
                        while (cr.isAfterLast() == false) {
                            String key = cr.getString(0);
                            String value = cr.getString(1);
                            res = res + key + ":" + value + "|";
                            cr.moveToNext();

                        }
                        count = 0;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERYRESPONSE", res, add, mport, String.valueOf(c));
                    }
                } else if (messageType.equalsIgnoreCase("QUERYSTAR")) {
                    if (add.equalsIgnoreCase(sfirst_en)) {

                        MatrixCursor max = new MatrixCursor(new String[]{"key", "value"});
                        String[] strArray = message.split("\\|");
                        for (String str : strArray) {
                            if (str.contains(":")) {
                                MatrixCursor.RowBuilder bg = max.newRow();
                                bg.add("key", str.split(":")[0]);
                                bg.add("value", str.split(":")[1]);
                                slf = str.split(":")[1];
                                Log.v("key", str.split(":")[0]);
                                Log.v("value", str.split(":")[1]);
                            }
                        }
                        max.moveToFirst();
                        cr = max;
                        max.close();
                        flag = false;

                    } else {

                        sfirst_en = add;
                        result = message;
                        scount = Integer.parseInt(breader.readLine());

                        cr = query(mUri, null, "\"*\"", null, null);

                    }

                    scount = 0;

                }

                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (Exception e) {
                System.out.println("Exception is:" + e);
                e.printStackTrace();
            }
            return null;
        }
    }

    //----------------------------------CLIENT CODE----------------------------
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                System.out.println("InClient" + msgs);
                String messageType = msgs[0];
                String message = msgs[1];
                String mport = msgs[2];
                if (messageType.equalsIgnoreCase("JOIN")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mport));
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    out.println(messageType);
                    out.println(message);
                    out.println(mport);
                    out.flush();
                    socket.close();
                } else if (messageType.equalsIgnoreCase("SET PRE")) {
                    String add = msgs[3];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mport));
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    out.println(messageType);
                    out.println(message);
                    out.println(mport);
                    out.println(add);
                    out.flush();
                    socket.close();
                } else if (messageType.equalsIgnoreCase("INSERT")) {
                    String add = msgs[3];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mport));
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    out.println(messageType);
                    out.println(message);
                    out.println(mport);
                    out.println(add);
                    out.flush();
                    socket.close();
                } else if (messageType.equalsIgnoreCase("QUERYRESPONSE")) {
                    String add = msgs[3];
                    String count = msgs[4];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mport));
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    out.println(messageType);
                    out.println(message);
                    out.println(mport);
                    out.println(add);
                    out.println(count);
                    out.flush();
                    socket.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "Exception encountered");
            }
            return null;
        }
    }

    private class ClientTaskQ extends AsyncTask<String, Void, Boolean> {

        @Override
        protected Boolean doInBackground(String... msgs) {
            try {
                String messageType = msgs[0];
                String message = msgs[1];
                String mport = msgs[2];
                String count = msgs[4];
                String add = msgs[3];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mport));
                PrintWriter out = new PrintWriter(socket.getOutputStream());
                out.println(messageType);
                out.println(message);
                out.println(mport);
                out.println(add);
                out.println(count);
                out.flush();
                socket.close();

                flag = true;
                if (count.equalsIgnoreCase("1")) {
                    while (flag) {
                    }
                };
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "Exception encountered");
            }
            return false;
        }
    }
}
