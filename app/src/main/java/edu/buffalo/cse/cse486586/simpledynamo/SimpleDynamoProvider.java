package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	public static String TAG = SimpleDynamoProvider.class.getSimpleName();

	/*
	Variables
	 */

	/*
	Networking variables
	 */
	Node self = new Node();
	public String selfPortNumber_STR;
	public int selfPortNumber_INT;
	public String selfPortHash_STR;

	static final String [] REMOTE_PORT = {"5554","5556","5558","5560","5562"};
	static final String [] SORTED_PORTS = {"5562","5556","5554","5558","5560"};
    static final String [] SORTED_PORT_HASHES = {"177ccecaec32c54b82d5aaafc18a2dadb753e3b1",
            "208f7f72b198dadd244e61801abe1ec3a4857bc9",
            "33d6357cfaaf0f72991b0ecd8c56da066613c089",
            "abf0fd8db03e5ecb199a9b82929e9db79b909643",
            "c25ddd596aa7c81fa12378fa725f706d54325d12"};
    static final int SERVER_PORT = 10000;

    /*
    Database variables
     */
    DatabaseHelper databaseHelper;

    /*
    Logic variables
     */
    public static String delimiter = "###";
    public static String delimiterTwo = "&&";
    LinkedList <Node> nodeLL = new LinkedList<Node>();

    //---------------------------------------------------------------

    /*
	Methods
	 */
	public int getPortNumber(Context context) {
        /*
        returns the port number of the self, i.e. the port number of the AVD that the code is running on
         */
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4); //in the 5500s
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2)); //in the 11000s
		return Integer.parseInt(portStr);
	}
	/* Not sure what I thought while making this function:-
	public Node[] createNeighborArray(Node node) {
		ArrayList<Node> resultArrList = new ArrayList<Node>();
		String hash;

		for(String port: REMOTE_PORT) {
			if(!port.equals(selfPortNumber_STR)) {
				Log.d(TAG," "+port);
			}
		}
		return null;
	}
	*/
	public static ArrayList<String> getReplicators(String port) {
		int index = -1;
		for(int i=0; i<SORTED_PORTS.length; i++) {
			if(SORTED_PORTS[i].equals(port)) {
				index=i;
				break;
			}
		}
		ArrayList <String> resultAL = new ArrayList<String> ();
		if(index == SORTED_PORTS.length-2) {
			resultAL.add(SORTED_PORTS[index+1]);
			resultAL.add(SORTED_PORTS[0]);
		} else if (index == SORTED_PORTS.length-1) {
			resultAL.add(SORTED_PORTS[0]);
			resultAL.add(SORTED_PORTS[1]);
		} else {
			resultAL.add(SORTED_PORTS[index+1]);
			resultAL.add(SORTED_PORTS[index+2]);
		}
		return resultAL;
	}

    public static String returnMsgPosition (String messageText) {
        String msgHash="";
        try {
            msgHash = genHash(messageText);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int result_index = 0;

        for(int i=SORTED_PORT_HASHES.length-1; i>=0; i--) {
            if(msgHash.compareTo(SORTED_PORT_HASHES[i]) == 1) {
                break;
            } else if(msgHash.compareTo(SORTED_PORT_HASHES[i]) < 1) {
                result_index=i;
            }
        }
        System.out.println("\n"+result_index+" "+SORTED_PORTS[result_index]);
        return ""+SORTED_PORTS[result_index];
    }

    public static ArrayList<String> WhoseDataShouldIQueryFor(String port) {
        String [] SORTED_PORTS = {"5562","5556","5554","5558","5560"};
        ArrayList <String> resultAL =
                new ArrayList <String> (Arrays.asList("5562","5556","5554","5558","5560"));
        int position = resultAL.indexOf(port);

        ArrayList <String> resAL = new ArrayList <String> ();
        resAL.add(SORTED_PORTS[position]);
        resAL.add(SORTED_PORTS[(position + 3)%5]);
        resAL.add(SORTED_PORTS[(position + 4)%5]);

        System.out.println("I am: "+port+" and I store the data of ports: ");
        for(String a:resAL) {
            System.out.print(a+" ");
        }
        return resAL;
    }

    public String getPopularValue(ArrayList<String> inputAL) {
        String one = inputAL.get(0);
        String two = "";
        String three = "";

        if(inputAL.size()>1) {
            two = inputAL.get(1);
            if(inputAL.size()>2) {
                three = inputAL.get(2);
            }
        }

        if(one.equals(two)) {
            return one;
        }
        if(inputAL.size()>2) {
            if(one.equals(three)) {
                return one;
            } else if(two.equals(three)) {
                return two;
            }
        }
        return "";
    }



	//----------------------------
	@Override
	public boolean onCreate() {
		Log.d(TAG, "inside oncreate");

		/*
		Set self node:
		portNumber
		HashID
		address or selfPortNumber_INT
		 */
		selfPortNumber_INT = getPortNumber(this.getContext());
		selfPortNumber_STR = String.valueOf(selfPortNumber_INT);

		try {
			selfPortHash_STR = genHash(selfPortNumber_STR);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		self.setPortNumber(selfPortNumber_STR);
		self.setPortHashID(selfPortHash_STR);


        // -------------
		databaseHelper = new DatabaseHelper(getContext());
		Log.d(TAG, "returning from onCreate");


        // -------------
        try {
            ServerSocket socket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Log.d("OnCreate","recovering at AVD "+self.getPortNumber());

        /*
        get whose data I am storing
        make a query string
        send unicast message to
         */
        ArrayList <String> whoseDataDoIStore = WhoseDataShouldIQueryFor(self.getPortNumber());
        // 5554, 5562, 5556

        // quickQuery for master node replicas
        String FirstReplicatorOfMaster = getReplicators(whoseDataDoIStore.get(0)).get(0);
        //5558
        String query1 = "quickquery"+delimiter+self.getPortNumber()+delimiter+self.getPortNumber()+delimiterTwo+FirstReplicatorOfMaster;
        Log.d("OnCreate","send message "+query1+" to port "+FirstReplicatorOfMaster);




        String key = "";
        String value = "";

        String responseMaster = "";
        String responseBack2 = "";
        String responseBack1 = "";


        String back2 = whoseDataDoIStore.get(1);
        String back1 = whoseDataDoIStore.get(2);

        String query2 = "quickquery"+delimiter+back2+delimiter+self.getPortNumber()+delimiterTwo+back2;
        String query3 = "quickquery"+delimiter+back1+delimiter+self.getPortNumber()+delimiterTwo+back1;


        try {
            Log.d("recovery","sending query "+query1+" to "+FirstReplicatorOfMaster+". I am "+self.getPortNumber());
            responseMaster = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query1, self.getPortNumber()).get();
            Log.d("recovery","sending query "+query2+" to "+back2+". I am "+self.getPortNumber());
            responseBack2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query2, self.getPortNumber()).get();
            Log.d("recovery","sending query "+query3+" to "+back1+". I am "+self.getPortNumber());
            responseBack1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query3, self.getPortNumber()).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


//        responseMaster = (String) unicastWithResponse(query1,FirstReplicatorOfMaster);
//        //will be received as string
//
//        String back2 = whoseDataDoIStore.get(1);
//        String back1 = whoseDataDoIStore.get(2);

//        String query2 = "quickquery"+delimiter+back2;
//        String query3 = "quickquery"+delimiter+back1;
//
//        String responseBack2 = (String) unicastWithResponse(query2, back2);
//        String responseBack1 = (String) unicastWithResponse(query3, back1);
//        // got a response from the 5562 (responseBack2) (query2) and 5556 (responseBack3) (query3)
//
//
//        String key = "";
//        String value = "";
        if(responseMaster.length() > 0) {
            String[] arr1 = responseMaster.split(delimiterTwo);
            //keys and values tuple separated
            for(String tuple: arr1) {
                key = tuple.split(delimiter)[0];
                value = tuple.split(delimiter)[1];

                Log.d("recovery","received while recovery: key "+key+" value "+value);
                customInsertAtSelfNode(self.getPortNumber(), key, value);
            }
        }

        //master node data stored

        //now stored predecessor data
        if(responseBack2.length() > 0) {
            String[] arr2 = responseBack2.split(delimiterTwo);
            Log.d("OnCreate",arr2[0]+" is te zeroth ");
            for(String tuple: arr2) {
                Log.d("OnCreate","tuple is "+tuple);
                key = tuple.split(delimiter)[0];
                value = tuple.split(delimiter)[1];

                Log.d("OnCreate","inserting for back2's replication: key "+key+" value "+value);
                customInsertAtSelfNode(back2, key, value);
            }
        }

        if(responseBack1.length() > 0) {
            String[] arr3 = responseBack1.split(delimiterTwo);
            for(String tuple:arr3) {
                key = tuple.split(delimiter)[0];
                value = tuple.split(delimiter)[1];

                Log.d("OnCreate","inserting for back1's replication: key "+key+" value "+value);
                customInsertAtSelfNode(back1, key, value);
            }
        }

        return false;
	}







	@Override
	public synchronized Uri insert(Uri uri, ContentValues contentValues) {
		// TODO Auto-generated method stub
		Log.d(TAG,"inside insert");

        String[] valuesArr = printContentValues(contentValues);
        String key = valuesArr[3];
        String value = valuesArr[1];

        Log.d("Insert","Got insert request for key "+key+" and value "+value);

        String msgPositionPort = returnMsgPosition(key);

        // deleted some part here
        Log.d("INSERT","Will insert key: "+key+" with value: +"+value+" in node with deviceid "+msgPositionPort);
        String message = "insert" + delimiter +
                msgPositionPort + delimiter +
                key + delimiter +
                value;
        Boolean useless_return = unicastWithoutResponse(message, msgPositionPort);



        // inserting at master i.e. non replicating node worked
        // now implement replication at next two nodes.
        ArrayList<String> replicators = getReplicators(msgPositionPort);
        for (String replicatorPort: replicators) {
            Log.d("Insert","Sending replicated insert msg key "+key+" and value "+value+" to port "+replicatorPort);
            useless_return = unicastWithoutResponse(message, replicatorPort);
        }

		return null;
	}








	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.d("Query","inside query with query: "+selection);
        SQLiteDatabase db = databaseHelper.getReadableDatabase();
		Cursor cursor = null;
        if(selection.equals("@")) {
			Log.d("Query","@ called");
			cursor = db.rawQuery("select key, value from entry", null);
            return cursor;
        } else if(selection.equals("*")) {

            SQLiteDatabase db2 = databaseHelper.getReadableDatabase();
            Cursor cursor1 = null;
            cursor1 = db2.rawQuery("select key, value from entry", null);

            String message = "query*"+delimiter;

            MatrixCursor tempCursor = new MatrixCursor(new String[]{"key","value"});
            MergeCursor resultantCursor;

            for(int i=0; i<SORTED_PORTS.length; i++) {
                String response = "";
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(SORTED_PORTS[i])*2);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(message);
                    dos.flush();

                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    response = dis.readUTF();
                    dis.close();
                    dos.close();
                    socket.close();

                    Log.d("Query*","response from * query for port "+SORTED_PORTS[i]+" is "+response);
                    String[] responArr = response.split(delimiterTwo);
                    for(String tuple: responArr) {
                        Log.d("Query*","the tuple is "+tuple);
                        String key = tuple.split(delimiter)[0];
                        String value = tuple.split(delimiter)[1];
                        Log.d("Query*","got key value pair as key: "+key+" and value: "+value);
                        tempCursor.addRow(new String[]{key, value});
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (EOFException e) {
                    Log.d("Query*","EOF Exception occured while querying * to port "+SORTED_PORTS[i]);
                    continue;
                } catch (IOException e) {
                    e.printStackTrace();
                }


            }
            resultantCursor = new MergeCursor(new Cursor[]{cursor1, tempCursor});
            return resultantCursor;
        } else {

            /*
            init a "quorum" arraylist datastructure
            find which node should contain that key
            init for loop
                query the main node
                    store the result in the AL quorum
                query the get(replicators)
                    store in the AL quorum
             find the most frequent value in the quorum datastructure
             make cursor with that value
             return cursor
             */

            String key = selection;
            String portToSendQueryTo = returnMsgPosition(key);
            String message = "query"+delimiter+
                    portToSendQueryTo+delimiter+
                    key;

            ArrayList<String> masterAndReplicators = new ArrayList<String> ();
            masterAndReplicators.add(portToSendQueryTo);
            masterAndReplicators.addAll(getReplicators(portToSendQueryTo));

            String responseFromQuery = "";

            Log.d("Query","Master is "+masterAndReplicators.get(0)+" and replicators are "+
                    masterAndReplicators.get(1)+
                    " and "+masterAndReplicators.get(2));


            ArrayList<String> quorum = new ArrayList<String>();

            for(int i=0; i<masterAndReplicators.size(); i++) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(masterAndReplicators.get(i))*2);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(message);
                    dos.flush();
                    Log.d("Query","asking port "+masterAndReplicators.get(i)+" and sending message "+message);

                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    responseFromQuery = dis.readUTF();

                    dos.close();
                    dis.close();
                    socket.close();
                    Log.d("Query","Got response to query from port "+masterAndReplicators.get(i)+" as "+responseFromQuery+"---");
                    if(responseFromQuery.length() == 0) {
                        Log.d("Query","response after query from port "+masterAndReplicators.get(i)+" is of length 0. I am "+self.getPortNumber());
                        continue;
                    }
                    quorum.add(responseFromQuery);
                } catch (EOFException e) {
                    e.printStackTrace();
                    Log.d("Query", "EOFException occured while looking for AVD "+masterAndReplicators.get(i)+" so now will skip over it to next AVD");
                    continue;
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            String frequentValue = getPopularValue(quorum);

            String[] columns = {"key", "value"};
            MatrixCursor matrixCursor = new MatrixCursor(columns);
            Object[] row = new Object[matrixCursor.getColumnCount()];
            row[matrixCursor.getColumnIndex(databaseHelper.COLUMN_NAME_TITLE)] = key;
            row[matrixCursor.getColumnIndex(databaseHelper.COLUMN_NAME_SUBTITLE)] = frequentValue;
            matrixCursor.addRow(row);
            matrixCursor.moveToFirst();
            Log.d("Query","Returning key "+key+" and value "+responseFromQuery);
            return matrixCursor;
		}
		//Log.d("Query","Some problem in query, the query input is weird");
		//return null;
	}






    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        final String DROP_ALL_QUERY = "DROP TABLE IF EXISTS ENTRY";

        if(selection.equals("@")) {
            SQLiteDatabase sqLiteDatabase = databaseHelper.getWritableDatabase();
            sqLiteDatabase.execSQL(DROP_ALL_QUERY);
        } else if(selection.equals("*")) {
            broadcastWithoutResponse("delete"+delimiter+DROP_ALL_QUERY);
        } else {
            String query = "delete"+delimiter+"delete from entry where key='"+selection+"'";
            ArrayList<String> masterAndReplicators = new ArrayList<String>();
            String masterPort = returnMsgPosition(selection);
            masterAndReplicators.add(masterPort);
            masterAndReplicators.addAll(getReplicators(masterPort));

            Log.d("Delete","Key "+selection+" resides at ports "+masterAndReplicators);

            for(String port: masterAndReplicators) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port)*2);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(query);
                    dos.flush();

                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    String ack = dis.readUTF();
                    dis.close();
                    dos.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (EOFException eof) {
                    Log.d("Delete","EOFException occured while trying to send delete key message to port "+port);
                    continue;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return 0;
    }









    public static Object unicastWithResponse(String message, String port) throws EOFException {
        Object response = null;

        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port)*2);

            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF(message);
            dos.flush();

            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            response = ois.readObject();
            ois.close();
            dos.close();
            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return response;
    }





    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... strings) {
            String input = strings[0];
            String[] arr = input.split(delimiterTwo);
            String message = arr[0];
            String receiverPort = arr[1];

            Log.d("ClientTask","sending message "+message+" to port "+receiverPort);

            String response = "";

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(receiverPort)*2);

                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                dos.writeUTF(message);
                dos.flush();

                DataInputStream dis = new DataInputStream(socket.getInputStream());
                response = dis.readUTF();
                dis.close();
                dos.close();
                socket.close();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return response;
        }
    }






	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            try {
                ServerSocket socket = serverSockets[0];
                while(true) {
                    try {
                        Socket rocket = socket.accept();

                        DataInputStream dis = new DataInputStream(rocket.getInputStream());
                        String input = dis.readUTF();
                        Log.d("ServerTask","read from socket: "+input);

                        String[] inputArr = input.split(delimiter);

                        if(inputArr[0].equals("insert")) {
                            Log.d("ServerTask","Got an insert request at "+self.getPortNumber()+" with key "+inputArr[2]+", value "+inputArr[3]+" and deviceid "+inputArr[1]+" to store in node "+inputArr[1]);
                            if(inputArr[1].length() < 1) {
                                Log.d("Servertask","the deviceid has not been specified");
                            }
                            customInsertAtSelfNode(inputArr[1], //port
                                    inputArr[2], //key
                                    inputArr[3] //value
                            );

                            DataOutputStream dos = new DataOutputStream(rocket.getOutputStream());
                            dos.writeUTF("close");
                            dos.flush();
                            dos.close();
                            dis.close();
                            rocket.close();
                        } else if(inputArr[0].equals("query")) {
                            //"query" + delimiter + portToSendQueryTo + delimiter + key;
                            String selection = inputArr[2];
                            Log.d("ServerTask","Got a query request asking for key: "+selection+" which should be at master: "+inputArr[1]);

                            SQLiteDatabase db = databaseHelper.getReadableDatabase();
                            String queryString = "select value from entry where key=?";
                            Cursor cursor = db.rawQuery(queryString, new String[]{selection}, null);

                            String result = "";
                            DataOutputStream dos = new DataOutputStream(rocket.getOutputStream());
                            if(cursor.moveToFirst())
                                result = cursor.getString(cursor.getColumnIndex("value"));
                            cursor.close();

                            if(result.length() == 0) {
                                Cursor cursor2 = db.rawQuery("select value from entry where value = "+selection, null);
                                if(cursor.moveToFirst()) {
                                    result = cursor2.getString(cursor.getColumnIndex("value"));
                                }
                                cursor2.close();
                            }

                            Log.d("ServerTask","Sending response to the query for key: "+selection+" as the value "+result);
                            dos.writeUTF(result);
                            dos.flush();
                            dos.close();
                            dis.close();
                            rocket.close();
                        } else if(inputArr[0].equals("query*")) {
                            Log.d("SERVERT QUERY*","entered");
                            SQLiteDatabase sqLiteDatabase = databaseHelper.getReadableDatabase();
                            Cursor cursor = sqLiteDatabase.rawQuery("select key, value from Entry", null);
                            Log.d("SERVERT QUERY*","got readable database");

                            StringBuilder stringBuilder = new StringBuilder();

                            //Attribute: http://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                            String key = "";
                            String value = "";
                            while (cursor.moveToNext()) {
                                key = cursor.getString(cursor.getColumnIndex(databaseHelper.COLUMN_NAME_TITLE));
                                value = cursor.getString(cursor.getColumnIndex(databaseHelper.COLUMN_NAME_SUBTITLE));
                                Log.d("SERVERT QUERY* keyval","k "+key);
                                Log.d("SERVERT QUERY*","value "+value);
                                stringBuilder.append(key+delimiter+value+delimiterTwo);
                            }
                            //attribution over
                            cursor.close();
                            if(stringBuilder.length() > 0) {
                                stringBuilder.setLength(stringBuilder.length()-delimiterTwo.length());
                            }


                            DataOutputStream dos = new DataOutputStream(rocket.getOutputStream());
                            dos.writeUTF(stringBuilder.toString());
                            dos.flush();

                            dis.close();
                            dos.close();
                            rocket.close();
                        } else if(inputArr[0].equals("delete")) {
                            Log.d("ServerTask","got a delete request with message: "+inputArr[1]);
                            String query = inputArr[1];

                            SQLiteDatabase sqLiteDatabase = databaseHelper.getWritableDatabase();
                            sqLiteDatabase.execSQL(query);

                            DataOutputStream dos = new DataOutputStream(rocket.getOutputStream());
                            dos.writeUTF("close");
                            dos.flush();
                            dos.close();
                            dis.close();
                            rocket.close();
                        } else if(inputArr[0].equals("quickquery")) {
                            // quickquery + delimiter + port
                            Log.d("Servertask recovery","got recovery request message as "+inputArr[1]+" from "+inputArr[2]);
                            String port = inputArr[1];

                            String query = "select key, value from entry where key='"+port+"'";

                            StringBuilder stringBuilder = new StringBuilder();

                            SQLiteDatabase db = databaseHelper.getReadableDatabase();
                            Cursor cursor = db.rawQuery("select key, value from entry where deviceid='"+port+"'", null);

                            while(cursor.moveToNext()) {
                                String key = cursor.getString(cursor.getColumnIndex(databaseHelper.COLUMN_NAME_TITLE));
                                String value = cursor.getString(cursor.getColumnIndex(databaseHelper.COLUMN_NAME_SUBTITLE));
                                Log.d("quickquery","sending back recovery key: "+key+" value: "+value+". I am "+self.getPortNumber());

                                stringBuilder.append(key+delimiter+value+delimiterTwo);
                            }
                            cursor.close();
                            if(stringBuilder.length() > 0) {
                                stringBuilder.setLength(stringBuilder.length()-delimiterTwo.length());
                            }

                            DataOutputStream dos = new DataOutputStream(rocket.getOutputStream());
                            dos.writeUTF(stringBuilder.toString());
                            dos.flush();
                            dos.close();
                            dis.close();
                            rocket.close();
                        }
                    } catch (IOException io) {
                        Log.d("Exception","exception IOexception occured in ServerTask");
                    }
                }
            } catch(Exception e) {
                Log.d("Exception","exception occured in ServerTask");
                e.printStackTrace();
            }
            return null;
        }
    }


















    private void customInsertAtSelfNode(String port, String key, String value) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(DatabaseHelper.COLUMN_NAME_TITLE,key);
        contentValues.put(DatabaseHelper.COLUMN_NAME_SUBTITLE,value);
        contentValues.put(DatabaseHelper.COLUMN_NAME_RECEIVED_FROM,port);

        SQLiteDatabase sqLiteDatabase = databaseHelper.getWritableDatabase();
        sqLiteDatabase.insertWithOnConflict(databaseHelper.TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
        Log.d("customInsert","inserted key "+key+" and value "+value+" into database");
    }


    public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	/*
    START ATTRIBUTION
    http://stackoverflow.com/questions/2390244/how-to-get-the-keys-from-contentvalues

    Taken with permission from the professor
     */
	public String[] printContentValues(ContentValues values)
	{
		Set<Map.Entry<String, Object>> s=values.valueSet();
		Iterator itr = s.iterator();

		ArrayList<String> result_AL = new ArrayList<String>();

//        Log.d("DatabaseSync", "ContentValue Length :: " +values.size());

		while(itr.hasNext())
		{
			Map.Entry me = (Map.Entry)itr.next();
			String key = me.getKey().toString();
			Object value =  me.getValue();

            Log.d("DatabaseSync", "trying to insert Key:"+key+", values:"+(String)(value == null?null:value.toString()));

			result_AL.add(key);
			result_AL.add((String)(value == null?null:value.toString()));
		}
		String[] result = result_AL.toArray(new String[result_AL.size()]);

		return result;
	}
    /*
    Taken with permission from the professor

    END
     */

    public String sendMessageTo(String msgInput, String port) {
        String response = "";
        String[] msgArr = msgInput.split(delimiter);
        if(msgArr[0].equals("insert")) {
            //"insert" + delimiter + msgPosition + delimiter + key + delimiter + value;
            Log.d("SendMessage Method", "got a insert request to send key "+msgArr[2]+" and value "+msgArr[3]+" to "+port);
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port)*2);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                dos.writeUTF(msgInput);
                dos.flush();

                DataInputStream dis = new DataInputStream(socket.getInputStream());
                try {
                    response = dis.readUTF();
                    if(response.equals("close")) {
                        dis.close();
                        dos.close();
                        socket.close();
                    }
                } catch(EOFException eof) {
                    Log.d(TAG, "EOF Exception occured");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Log.d("SendMessage Method", "ACK received is "+response);
        return response;
    }

    public static String sendQueryAndGetResponseAsString(String msgInput, String port) throws  EOFException{
        String response = "";
        String[] msgArr = msgInput.split(delimiter);
        //"query" + delimiter + portToSendQueryTo + delimiter + key;

        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port)*2);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF(msgInput);
            dos.flush();

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            response = dis.readUTF();

            dos.close();
            dis.close();
            socket.close();
            //will read String from the socket. Will convert it to a cursor in a different function from right here
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return response;
    }

    public static HashMap<String, String> queryAll() {
        String message = "query*";
        HashMap<String, String> resultHM = new HashMap<String, String>();

        try {
            for(int i=0; i<SORTED_PORTS.length; i++) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(SORTED_PORTS[i])*2);

                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                dos.writeUTF(message);
                dos.flush();

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Object response = ois.readObject();
                ois.close();
                dos.close();
                socket.close();

                String[] responseArr = (String []) response;
                for(String line:responseArr) {
                    String[] lineSplit = line.split(delimiter);

                    //add conflict resolution code here
                    resultHM.put(lineSplit[0], lineSplit[1]);
                }

            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return resultHM;
    }



    //---------------------------------------------------------------------
    public void broadcastWithoutResponse(String message) {
        try {
            for(int i=0; i<SORTED_PORTS.length; i++) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(SORTED_PORTS[i])*2);

                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                dos.writeUTF(message);
                dos.flush();

                DataInputStream dis = new DataInputStream(socket.getInputStream());
                String ack = dis.readUTF();
                dis.close();
                dos.close();
                socket.close();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Boolean unicastWithoutResponse(String message, String port) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port)*2);

            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF(message);
            dos.flush();

            Log.d("Unicast","sending message "+message+" to port "+port);

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String ack = dis.readUTF();
            dis.close();
            dos.close();
            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
















    /*
    Don't go beyond
     */

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
}
