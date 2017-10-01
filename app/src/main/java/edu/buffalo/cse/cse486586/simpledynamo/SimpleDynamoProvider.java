package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadFactory;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final String Content_URI = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
    static final Uri URI = Uri.parse(Content_URI);
    String successor_node1 = "";
    String successor_node2 = "";
    String predecessor_node = "";
    String predecessor_hash;
    String successor1_hash;
    String successor2_hash;
    String current_port_hash;
    static final String REMOTE_PORT[] = {"11108", "11112", "11116", "11120", "11124"};
    static final String port[] = {"5554", "5556", "5558", "5560", "5562"};
    static final int SERVER_PORT = 10000;
    static final String KEY_FIELD = "key";
    static final String VALUE_FIELD = "value";
    static String avds[] = new String[5];
public String failed_avd="";
    String myPort = "";
    String portStr;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        try {
            Context c = this.getContext();
            String files[] = c.fileList();
            if (selection.equals("@") || selection.equals("/@/")) {

                for (int i = 0; i < files.length; i++) {
                    c.deleteFile(files[i]);
                }

            } else {
                int count = 0;
                //check if key exists in current node
                for (int i = 0; i < files.length; i++) {
                    if (files[i].equals(selection)) {
                        count += 1;
                    }
                }
                if ((((genHash(selection).compareTo(predecessor_hash) > 0) && (genHash(selection).compareTo(current_port_hash) < 0)) || (((genHash(selection).compareTo(predecessor_hash) > 0) || (genHash(selection).compareTo(current_port_hash) < 0)) && (predecessor_hash.compareTo(current_port_hash) > 0) && (successor1_hash.compareTo(current_port_hash) > 0))) || (count > 0)) {

                    c.deleteFile(selection);
                } else {
                    if ((genHash(selection).compareTo(genHash(avds[4])) > 0) || (genHash(selection).compareTo(genHash(avds[0])) < 0)) {
                        Log.d("In forward delete", genHash(avds[0]) + ":" + selection + ":" + genHash(selection));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[0]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Delete" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        if (in.readUTF().equals("ACK")) {
                            out.flush();
                            out.close();
                            socket.close();
                        }

                    } else if ((genHash(selection).compareTo(genHash(avds[3])) > 0) && (genHash(selection).compareTo(genHash(avds[4])) < 0)) {
                        Log.d("In forward delete", genHash(avds[4]) + ":" + selection + ":" + genHash(selection));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[4]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Delete" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        if (in.readUTF().equals("ACK")) {
                            out.flush();
                            out.close();
                            socket.close();
                        }

                    } else if ((genHash(selection).compareTo(genHash(avds[2])) > 0) && (genHash(selection).compareTo(genHash(avds[3])) < 0)) {
                        Log.d("In forward delete", genHash(avds[3]) + ":" + selection + ":" + genHash(selection));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[3]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Delete" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        if (in.readUTF().equals("ACK")) {
                            out.flush();
                            out.close();
                            socket.close();
                        }


                    } else if ((genHash(selection).compareTo(genHash(avds[0])) > 0) && (genHash(selection).compareTo(genHash(avds[1])) < 0)) {
                        Log.d("In forward delete", genHash(avds[1]) + ":" + selection + ":" + genHash(selection));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[1]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Delete" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        if (in.readUTF().equals("ACK")) {
                            out.flush();
                            out.close();
                            socket.close();
                        }


                    } else if ((genHash(selection).compareTo(genHash(avds[1])) > 0) && (genHash(selection).compareTo(genHash(avds[2])) < 0)) {
                        Log.d("In forward delete", genHash(avds[2]) + ":" + selection + ":" + genHash(selection));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[2]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Delete" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        if (in.readUTF().equals("ACK")) {
                            out.flush();
                            out.close();
                            socket.close();
                        }

                    }


                }


            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.d("In insert", portStr);
        try {
            FileOutputStream outputStream;
            String key = values.getAsString("key");
            String value = values.getAsString("value");

            String key_hash = genHash(key);
            Log.d("at initial insert", "key " + key);
            Log.d("key " + key, " hash key " + key_hash);
            if (((key_hash.compareTo(predecessor_hash) > 0) && (key_hash.compareTo(current_port_hash) < 0)) || ((predecessor_hash.compareTo(current_port_hash) > 0) && (current_port_hash.compareTo(key_hash) > 0)) || ((predecessor_hash.compareTo(current_port_hash) > 0) && (key_hash.compareTo(predecessor_hash) > 0))) {

                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
                Log.d("Current port insert", portStr + ":" + key + ":" + value);
                Log.d("Successor 1", successor_node1);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_node1) * 2);
                    DataOutputStream out_insert = new DataOutputStream(socket.getOutputStream());
                    out_insert.writeUTF("Replicate 1" + ":" + key + ":" + value);
                    DataInputStream in_insert = new DataInputStream(socket.getInputStream());
                    String read_input = in_insert.readUTF();
                    Log.d("read input", read_input);
                    if (read_input.equals("ACK")) {
                        out_insert.flush();
                        out_insert.close();
                        socket.close();
                    }

                } catch (SocketTimeoutException st) {
                    failed_avd=successor_node1;
                    Log.d("current succ fail",failed_avd);
                    st.printStackTrace();
                }
                catch (IOException st) {
                    failed_avd=successor_node1;
                    Log.d("current succ fail",failed_avd);
                    st.printStackTrace();
                }
                if(!failed_avd.equals(successor_node1)){
                    Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(successor_node2)*2);
                    DataOutputStream out=new DataOutputStream(socket.getOutputStream());
                    out.writeUTF("Replicate 2"+":"+key+":"+value);
                    DataInputStream in=new DataInputStream(socket.getInputStream());
                    if(in.readUTF().equals("ACK")){
                        out.flush();
                        out.close();
                        socket.close();
                    }
                }

            } else {
                String successor[] = new String[2];
                String current="";
                try {
                    if ((key_hash.compareTo(genHash(avds[4])) > 0) || (key_hash.compareTo(genHash(avds[0])) < 0)) {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(avds[0])*2);
                        successor[0]=avds[1];
                        successor[1]=avds[2];
                        DataOutputStream out=new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Insert"+":"+key+":"+value);
                        socket.setSoTimeout(100);
                        DataInputStream in=new DataInputStream(socket.getInputStream());
                        current=avds[0];
                        if(in.readUTF().equals("ACK")){
                            out.flush();
                            out.close();
                            socket.close();
                        }
                    }
                    else if ((key_hash.compareTo(genHash(avds[0])) > 0) && (key_hash.compareTo(genHash(avds[1])) < 0)){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(avds[1])*2);
                        successor[0]=avds[2];
                        successor[1]=avds[3];
                        DataOutputStream out=new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Insert"+":"+key+":"+value);
                        socket.setSoTimeout(100);
                        DataInputStream in=new DataInputStream(socket.getInputStream());
                        current=avds[1];
                        if(in.readUTF().equals("ACK")){
                            out.flush();
                            out.close();
                            socket.close();
                        }
                    }
                    else if ((key_hash.compareTo(genHash(avds[1])) > 0) && (key_hash.compareTo(genHash(avds[2])) < 0)) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(avds[2])*2);
                        successor[0]=avds[3];
                        successor[1]=avds[4];
                        DataOutputStream out=new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Insert"+":"+key+":"+value);
                        socket.setSoTimeout(100);
                        DataInputStream in=new DataInputStream(socket.getInputStream());
                        current=avds[2];
                        if(in.readUTF().equals("ACK")){
                            out.flush();
                            out.close();
                            socket.close();
                        }
                    }
                    else if ((key_hash.compareTo(genHash(avds[2])) > 0) && (key_hash.compareTo(genHash(avds[3])) < 0)) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(avds[3])*2);
                        successor[0]=avds[4];
                        successor[1]=avds[0];
                        DataOutputStream out=new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Insert"+":"+key+":"+value);
                        socket.setSoTimeout(100);
                        DataInputStream in=new DataInputStream(socket.getInputStream());
                        current=avds[3];
                        if(in.readUTF().equals("ACK")){
                            out.flush();
                            out.close();
                            socket.close();
                        }
                    }

                    else if ((key_hash.compareTo(genHash(avds[3])) > 0) && (key_hash.compareTo(genHash(avds[4])) < 0)){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(avds[4])*2);
                        successor[0]=avds[0];
                        successor[1]=avds[1];
                        DataOutputStream out=new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Insert"+":"+key+":"+value);
                        socket.setSoTimeout(100);
                        DataInputStream in=new DataInputStream(socket.getInputStream());
                        current=avds[4];
                        if(in.readUTF().equals("ACK")){
                            out.flush();
                            out.close();
                            socket.close();
                        }
                    }

                    Log.d("In insert else ",current);

                }catch (IOException e){
                    failed_avd=current;
                    Log.d("In insert else","Failed avd is "+failed_avd);
                    e.printStackTrace();
                }
                if(failed_avd.equals(current)){
                    Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(successor[0])*2);
                    DataOutputStream out_insert = new DataOutputStream(socket.getOutputStream());
                    out_insert.writeUTF("Replicate 1" + ":" + key + ":" + value);
                    DataInputStream in_insert = new DataInputStream(socket.getInputStream());
                    String read_input = in_insert.readUTF();
                    Log.d("read input", read_input);
                    if (read_input.equals("ACK")) {
                        out_insert.flush();
                        out_insert.close();
                        socket.close();
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return uri;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            Map<String, String> sort = new TreeMap<String, String>();

            try {
                current_port_hash = genHash(portStr);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }


            for (int i = 0; i < 5; i++) {
                sort.put(genHash(port[i]), port[i]);

            }
            avds = sort.values().toArray(new String[sort.size()]);

            if(portStr.equals(avds[0])){
                successor_node1=avds[1];
                successor_node2=avds[2];
                predecessor_node=avds[4];
            }
            else if(portStr.equals(avds[1])){
                successor_node1=avds[2];
                successor_node2=avds[3];
                predecessor_node=avds[0];
            }
            else if(portStr.equals(avds[2])){
                successor_node1=avds[3];
                successor_node2=avds[4];
                predecessor_node=avds[1];
            }
            else if(portStr.equals(avds[3])){
                successor_node1=avds[4];
                successor_node2=avds[0];
                predecessor_node=avds[2];
            }
            else if(portStr.equals(avds[4])){
                successor_node1=avds[0];
                successor_node2=avds[1];
                predecessor_node=avds[3];
            }
            String[]files=getContext().fileList();
            if(files.length>0&&files!=null){
                for(int k=0;k<files.length;k++){
                    getContext().deleteFile(files[k]);
                }
                String str="Recover"+":"+myPort+":"+predecessor_node+":"+successor_node1;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, str, myPort);
            }
            else{
                String str="New"+":"+myPort+":"+predecessor_node+":"+successor_node1+":"+successor_node2;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, str, myPort);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            while (true) {
                ServerSocket serverSocket = sockets[0];
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream in_server = new DataInputStream(socket.getInputStream());
                    DataOutputStream out_of_server = new DataOutputStream(socket.getOutputStream());
                    String read = in_server.readUTF();
                    Log.d("server read", read + ":" + myPort);
                    String server_array[] = read.split(":");
                    if (server_array[0].equals("Recover")) {
                        String response = server_array[1]+":"+server_array[2] + ":" + server_array[3] ;
                        out_of_server.writeUTF(response);
                    }
                    else if(server_array[0].equals("New")){
                        String response=server_array[2]+":"+server_array[3]+":"+server_array[4];
                        out_of_server.writeUTF(response);
                    }
                    else if (server_array[0].equals("Acknowledgement")) {
                        out_of_server.flush();
                        out_of_server.close();
                        socket.close();
                    } else if (server_array[0].equals("Replicate 1")) {
                        Log.d("in replicate 1", "server of;" + portStr);
                        String response = replicate_msg1(server_array[1], server_array[2]);
                        out_of_server.writeUTF(response);
                        out_of_server.flush();
                        socket.close();
                    } else if (server_array[0].equals("Replicate 2")) {
                        Log.d("in replicate 2", "server of;" + portStr);
                        String response = replicate_msg2(server_array[1], server_array[2]);
                        out_of_server.writeUTF(response);
                        out_of_server.flush();
                        socket.close();

                    }
                    else if(server_array[0].equals("Recovery")){
                        String str="Recovery1"+":"+server_array[1]+":"+server_array[2]+":"+server_array[3];
                        publishProgress(str);
                        out_of_server.writeUTF("ACK"+":"+"");
                        out_of_server.close();
                        socket.close();
                    }


                    else if (server_array[0].equals("Insert")) {
                        String key = server_array[1];
                        String val = server_array[2];
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
                        uriBuilder.scheme("content");
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD, key);
                        cv.put(VALUE_FIELD, val);
                        Uri uri = uriBuilder.build();
                        insert(uri, cv);
                        out_of_server.writeUTF("ACK");
                        Thread.sleep(100);
                        out_of_server.close();
                        socket.close();

                    }
                    else if(server_array[0].equals("Recover succ")){
                        String response=recover_1(server_array[1],server_array[2]);
                        out_of_server.writeUTF(response);
                        out_of_server.flush();
                        socket.close();
                    }
                    else if(server_array[0].equals("Response")){
                        String str="Response"+":"+server_array[1]+":"+server_array[2];
                        out_of_server.writeUTF("ACK");
                        publishProgress(str);
                        out_of_server.close();
                        socket.close();
                    }

                    else if(server_array[0].equals("Recover pred")){
                        String response=recover_2(server_array[1]);
                        out_of_server.writeUTF(response);
                        out_of_server.flush();
                        socket.close();
                    }

                    else if (server_array[0].equals("Individual query")) {
                        Log.d("In server forward", portStr);
                        String query = individualQuery(server_array[1]);
                        out_of_server.writeUTF(query);

                        out_of_server.flush();
                        out_of_server.close();
                        socket.close();
                    } else if (server_array[0].equals("Query All")) {
                        Log.d("In server query all", portStr);
                        String response = queryAll(server_array[1], server_array[2]);
                        out_of_server.writeUTF(response);
                        out_of_server.flush();
                        socket.close();
                    } else if (server_array[0].equals("Delete")) {
                        String response = deleteOne(server_array[1]);
                        out_of_server.writeUTF(response);
                        out_of_server.flush();
                        socket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
        }

        protected void onProgressUpdate(String... strings) {

String strReceived=strings[0].trim();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strReceived, myPort);

        }
    }


    private String recover_2(String original){
        String response="";
        String keys="",values="";
        String files[]=getContext().fileList();
        if(files.length>0) {
            try {
                String str = "";
                for (int i = 0; i < files.length; i++) {
                    FileInputStream fis = getContext().openFileInput(files[i]);
                    InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                    BufferedReader br = new BufferedReader(isr);

                    str = br.readLine();


                    keys = keys + "-" + files[i];
                    values = values + "-" + str;

                    fis.close();
                    isr.close();
                }
                if (keys.length() > 0) {
                    keys = keys.substring(1);
                    values = values.substring(1);
                }
                response = keys + ":" + values;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return response;

    }





    private String deleteOne(String selection) {

        Context c = this.getContext();
        c.deleteFile(selection);
        String response = "ACK";


        return response;


    }
    private String recover_1(String original,String predecessor){
        String response="";
        String received=null;
        String keys="",values="";
        try{
            Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(predecessor)*2);
            DataOutputStream out=new DataOutputStream(socket.getOutputStream());
            out.writeUTF("Recover pred"+":"+original);
            DataInputStream in=new DataInputStream(socket.getInputStream());
            received=in.readUTF();
            if(received!=null){
                String[] key_value_array = received.split(":");
                if (key_value_array.length == 2) {
                    String[] keys_array = key_value_array[0].split("-");
                    String[] values_array = key_value_array[1].split("-");
                    for (int i = 0; i < keys_array.length; i++) {
                        keys = keys + "-" + keys_array[i];
                        values = values + "-" + values_array[i];

                    }
                }
            }
            String[] files = getContext().fileList();
            String str="";
            for (int i=0;i<files.length;i++){
                if(original.equals(String.valueOf(Integer.parseInt(avds[0])*2))){
                    if ((genHash(files[i]).compareTo(genHash(avds[4])) > 0) || (genHash(files[i]).compareTo(genHash(avds[0])) < 0)) {
                        FileInputStream fis = getContext().openFileInput(files[i]);
                        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                        BufferedReader br = new BufferedReader(isr);

                        str = br.readLine();
                        keys = keys + "-" + files[i];
                        values = values + "-" + str;

                        fis.close();
                        isr.close();

                    }
                }
               else if(original.equals(String.valueOf(Integer.parseInt(avds[4])*2))){
                    if ((genHash(files[i]).compareTo(genHash(avds[3])) > 0) && (genHash(files[i]).compareTo(genHash(avds[4])) < 0)) {
                        FileInputStream fis = getContext().openFileInput(files[i]);
                        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                        BufferedReader br = new BufferedReader(isr);

                        str = br.readLine();
                        keys = keys + "-" + files[i];
                        values = values + "-" + str;

                        fis.close();
                        isr.close();
                    }

                }
                else if(original.equals(String.valueOf(Integer.parseInt(avds[3])*2))){
                    if ((genHash(files[i]).compareTo(genHash(avds[2])) > 0) && (genHash(files[i]).compareTo(genHash(avds[3])) < 0)) {
                        FileInputStream fis = getContext().openFileInput(files[i]);
                        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                        BufferedReader br = new BufferedReader(isr);

                        str = br.readLine();
                        keys = keys + "-" + files[i];
                        values = values + "-" + str;

                        fis.close();
                        isr.close();
                    }
                }
                else if(original.equals(String.valueOf(Integer.parseInt(avds[1])*2))){
                    if ((genHash(files[i]).compareTo(genHash(avds[0])) > 0) && (genHash(files[i]).compareTo(genHash(avds[1])) < 0)) {
                        FileInputStream fis = getContext().openFileInput(files[i]);
                        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                        BufferedReader br = new BufferedReader(isr);

                        str = br.readLine();
                        keys = keys + "-" + files[i];
                        values = values + "-" + str;

                        fis.close();
                        isr.close();


                    }
                }
                else if(original.equals(String.valueOf(Integer.parseInt(avds[2])*2))){
                    if ((genHash(files[i]).compareTo(genHash(avds[1])) > 0) && (genHash(files[i]).compareTo(genHash(avds[2])) < 0)) {
                        FileInputStream fis = getContext().openFileInput(files[i]);
                        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                        BufferedReader br = new BufferedReader(isr);

                        str = br.readLine();
                        keys = keys + "-" + files[i];
                        values = values + "-" + str;

                        fis.close();
                        isr.close();
                    }
                }

            }
            if (keys.length() > 0) {
                keys = keys.substring(1);
                values = values.substring(1);
            }

            String key_values = keys + ":" + values;
            response="ACK";
            Socket socket1=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(original));
            DataOutputStream out1=new DataOutputStream(socket1.getOutputStream());
            out1.writeUTF("Response"+":"+key_values);
            DataInputStream in1=new DataInputStream(socket1.getInputStream());
            if(in1.readUTF().equals("ACK")){
                out1.close();
                socket1.close();
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

return response;
    }


    private String queryAll(String selection, String originating_node) {
        String received = null;
        String response = null;
        Log.d("Inside query all", predecessor_node + ":" + portStr + ":" + originating_node + ":" + successor_node1);
        try {
            String keys = "", values = "";
            if (!originating_node.equals(successor_node1)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_node1) * 2);

                String str = "Query All" + ":" + selection + ":" + originating_node;
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(str);
                DataInputStream in = new DataInputStream(socket.getInputStream());
                received = in.readUTF();
            }
            if (received != null) {
                String[] key_value_array = received.split(":");
                if (key_value_array.length == 2) {
                    String[] keys_array = key_value_array[0].split("-");
                    String[] values_array = key_value_array[1].split("-");
                    for (int i = 0; i < keys_array.length; i++) {
                        keys = keys + "-" + keys_array[i];
                        values = values + "-" + values_array[i];

                    }
                }
            }
            String[] files = getContext().fileList();
            // Log.d("length of files",String.valueOf(files.length));
            String str = "";
            for (int i = 0; i < files.length; i++) {
                Log.d("inside for", myPort);

                FileInputStream fis = getContext().openFileInput(files[i]);
                InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                BufferedReader br = new BufferedReader(isr);

                str = br.readLine();


                keys = keys + "-" + files[i];
                values = values + "-" + str;

                fis.close();
                isr.close();


            }
            if (keys.length() > 0) {
                keys = keys.substring(1);
                values = values.substring(1);
            }
            response = keys + ":" + values;
            //  Log.d("in query all", myPort + " " + response);


        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return response;
    }

    private String individualQuery(String selection) {

        String key_value = "";
        try {
            FileInputStream fis = getContext().openFileInput(selection);
            InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(isr);
            String response = bufferedReader.readLine();


            fis.close();
            isr.close();
            bufferedReader.close();
            key_value = selection + ":" + response;


        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return key_value;

    }

    private String replicate_msg2(String key, String value) {
        String response = "";
        try {
            Log.d("in replicate msg2", key + ":" + value);
            FileOutputStream outputStream;
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
            response = "ACK";
            Log.d("Replicate 2", predecessor_node + ":" + portStr + ":" + key + ":" + value);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    private String replicate_msg1(String key, String value) {
        String response = "";
        try {
            Log.d("in replicate msg1", key + ":" + value);
            FileOutputStream outputStream;
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
            Log.d("Replicate 1", predecessor_node + ":" + portStr + ":" + key + ":" + value);
            Log.d("sending to successor 2", successor_node1);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_node1) * 2);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF("Replicate 2" + ":" + key + ":" + value);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            String read_input = input.readUTF();
            if (read_input.equals("ACK")) {
                out.flush();
                out.close();
                socket.close();
            }
            response = "ACK";


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String msg = msgs[0];
                String msg_read[] = msg.split(":");
                if (msg_read[0].equals("New")) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg_read[1]));
                    DataOutputStream to_server = new DataOutputStream(socket.getOutputStream());

                    to_server.writeUTF(msg);
                    DataInputStream from_server = new DataInputStream(socket.getInputStream());
                    String read[] = from_server.readUTF().split(":");
                    successor_node1=read[1];
                    predecessor_node=read[0];
                    successor_node2=read[2];
                    predecessor_hash = genHash(predecessor_node);
                    successor1_hash = genHash(successor_node1);
                    successor2_hash = genHash(successor_node2);
                    Log.d("info of " + portStr, predecessor_node + ":" + successor_node1 + ":" + successor_node2);
                    to_server.writeUTF("Acnowledgement" + ":" + "");
                    from_server.close();
                    to_server.flush();
                    to_server.close();
                    socket.close();



                }
                else if(msg_read[0].equals("Recover")){
                    Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(msg_read[1]));
                    DataOutputStream to_server = new DataOutputStream(socket.getOutputStream());

                    to_server.writeUTF(msg);
                    DataInputStream from_server = new DataInputStream(socket.getInputStream());
                    String read[] = from_server.readUTF().split(":");
                    String current=read[0];
                    successor_node1=read[2];
                    predecessor_node=read[1];
                    to_server.writeUTF("Recovery"+":"+current+":"+predecessor_node+":"+successor_node1);
                    if(read[0].equals("ACK")){
                        to_server.flush();
                        to_server.close();
                        socket.close();
                    }

                }
                else if(msg_read[0].equals("Recovery 1")){
                    Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),Integer.parseInt(msg_read[3])*2);
                    DataOutputStream out=new DataOutputStream(socket.getOutputStream());
                    out.writeUTF("Recover succ"+":"+msg_read[1]+":"+msg_read[2]);
                    DataInputStream in=new DataInputStream(socket.getInputStream());
                    if(in.readUTF().equals("ACK")){
                        out.close();
                        socket.close();
                    }
                }
                else if(msg_read[0].equals("Response")){
                    String keys[]=msg_read[1].split("-");
                    String values[]=msg_read[2].split("-");
                    FileOutputStream outputStream;
                    for (int i=0;i<keys.length;i++){
                        outputStream = getContext().openFileOutput(keys[i], Context.MODE_PRIVATE);
                        outputStream.write(values[i].getBytes());
                        outputStream.close();
                    }
                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;

        }
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        // Log.d("In query", selection);
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        String originating_node = portStr;
        try {
            //Log.d("In @ query",portStr);
            if (selection.equals("@") || selection.equals("/@/")) {
                String[] files = getContext().fileList();
                Log.d("In @ query", portStr);

                String str = "";
                for (int i = 0; i < files.length; i++) {


                    FileInputStream fis = getContext().openFileInput(files[i]);
                    InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                    BufferedReader br = new BufferedReader(isr);

                    str = br.readLine();

                    fis.close();
                    isr.close();

                    cursor.addRow(new String[]{files[i], str});

                    Log.d("type@", " Value :" + str + " retrieved for key :" + files[i]);


                }
                return cursor;


            } else if (selection.equals("*") || selection.equals("/*/")) {
                Log.d("in type * query", portStr);

                String[] files = getContext().fileList();
                Log.d("FilesLength", Integer.toString(files.length));
                String str = "";
                for (int i = 0; i < files.length; i++) {


                    FileInputStream fis = getContext().openFileInput(files[i]);
                    InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                    BufferedReader br = new BufferedReader(isr);

                    str = br.readLine();

                    fis.close();
                    isr.close();
                    Log.d("in type * loop1", files[i] + str);

                    cursor.addRow(new String[]{files[i], str});
                }
                Log.d("After loop 1", "in *");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_node1) * 2);
                String to_Send = "Query All" + ":" + "*" + ":" + originating_node;
                Log.d("Sending to successor", to_Send);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(to_Send);
                DataInputStream in_query = new DataInputStream(socket.getInputStream());
                String from_server = in_query.readUTF();
                String key_values[] = from_server.split(":");
                String key_pos[] = key_values[0].split("-");
                if (key_pos.length > 0) {
                    Log.d("no.of rows in", myPort + key_pos);
                    String value_pos[] = key_values[1].split("-");
                    for (int i = 0; i < key_pos.length; i++) {
                        cursor.addRow(new String[]{key_pos[i], value_pos[i]});
                        Log.d("in type *", "in for loop");

                    }
                    socket.close();
                }
                return cursor;
            } else if (((genHash(selection).compareTo(predecessor_hash) > 0) && (genHash(selection).compareTo(current_port_hash) < 0)) || (((genHash(selection).compareTo(predecessor_hash) > 0) || (genHash(selection).compareTo(current_port_hash) < 0)) && (predecessor_hash.compareTo(current_port_hash) > 0) && (successor1_hash.compareTo(current_port_hash) > 0))) {
                Log.d("Current individual", genHash(selection) + ":" + genHash(portStr));
                String s = "";
                FileInputStream fis = getContext().openFileInput(selection);
                InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                BufferedReader bufferedReader = new BufferedReader(isr);
                s = bufferedReader.readLine();


                fis.close();
                isr.close();
                bufferedReader.close();
                cursor.addRow(new String[]{selection, s});


                cursor.setNotificationUri(getContext().getContentResolver(), uri);

                return cursor;
            } else {
                String[] key_value = new String[2];
                Log.d("FORWARDQUERY", selection);
                if ((genHash(selection).compareTo(genHash(avds[4])) > 0) || (genHash(selection).compareTo(genHash(avds[0])) < 0)) {
                    Log.d("In forward query", genHash(avds[0]) + ":" + selection + ":" + genHash(selection));
                    if(!failed_avd.equals(avds[2])){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[2]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");}
                    else{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[1]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");
                    }
                } else if ((genHash(selection).compareTo(genHash(avds[3])) > 0) && (genHash(selection).compareTo(genHash(avds[4])) < 0)) {
                    Log.d("In forward query", genHash(avds[4]) + ":" + selection + ":" + genHash(selection));
                    if(!failed_avd.equals(avds[1])){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[1]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");}
                    else{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[0]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");
                    }
                } else if ((genHash(selection).compareTo(genHash(avds[2])) > 0) && (genHash(selection).compareTo(genHash(avds[3])) < 0)) {
                    Log.d("In forward query", genHash(avds[3]) + ":" + selection + ":" + genHash(selection));
                    if(!failed_avd.equals(avds[0])){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[0]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");}
                    else{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[4]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");
                    }

                } else if ((genHash(selection).compareTo(genHash(avds[0])) > 0) && (genHash(selection).compareTo(genHash(avds[1])) < 0)) {
                    Log.d("In forward query", genHash(avds[1]) + ":" + selection + ":" + genHash(selection));
                    if(!failed_avd.equals(avds[3])){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[3]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");}
                    else {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[2]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");
                    }

                } else if ((genHash(selection).compareTo(genHash(avds[1])) > 0) && (genHash(selection).compareTo(genHash(avds[2])) < 0)) {
                    Log.d("In forward query", genHash(avds[2]) + ":" + selection + ":" + genHash(selection));
                    if(!failed_avd.equals(avds[4])){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[4]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");}
                    else {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avds[3]) * 2);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("Individual query" + ":" + selection);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        key_value = in.readUTF().split(":");
                    }

                }
                cursor.addRow(new String[]{key_value[0], key_value[1]});
                return cursor;
            }

        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        }
        // return cursor;


        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
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
}
