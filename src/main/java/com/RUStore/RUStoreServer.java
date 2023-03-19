package com.RUStore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static com.RUStore.Constant.BYTES_PER_INTEGER;
import static com.RUStore.Constant.KEY_ALREADY_EXIST;
import static com.RUStore.Constant.KEY_NOT_EXIST;
import static com.RUStore.Constant.KEY_SEPARATOR;
import static com.RUStore.Constant.SUCCESS;
import static com.RUStore.OperationCode.OPERATION_DISCONNECT;
import static com.RUStore.OperationCode.OPERATION_GET;
import static com.RUStore.OperationCode.OPERATION_LIST;
import static com.RUStore.OperationCode.OPERATION_PUT;
import static com.RUStore.OperationCode.OPERATION_REMOVE;

public class RUStoreServer
{
    private static volatile boolean running = true;
    private static final HashMap<String, byte[]> storedObjects = new HashMap<>();

    public static void main ( String[] args )
            throws IOException, InterruptedException
    {
        // Check if at least one argument that is potentially a port number
        if ( args.length != 1 )
        {
            System.out.println( "Invalid number of arguments. You must provide a port number." );
            return;
        }

        // Try and parse port # from argument
        int port = Integer.parseInt( args[0] );
        try ( ServerSocket serverSocket = new ServerSocket( port ) )
        {
            System.out.println( "RUStore server started on port " + port );

            while ( running )
            {
                System.out.println( "Waiting for client connection..." );

                try ( Socket socket = serverSocket.accept();
                      OutputStream out = new BufferedOutputStream( socket.getOutputStream() );
                      InputStream in = new BufferedInputStream( socket.getInputStream() ) )
                {
                    System.out.println( "Client connected from " + socket.getInetAddress().getHostAddress() );

                    boolean clientConnected = true;
                    while ( clientConnected )
                    {
                        // Get the operation code from the client.
                        byte[] request = new byte[BYTES_PER_INTEGER];
                        in.read( request );
                        int operationCode = ByteBuffer.wrap( request ).getInt();

                        if ( operationCode == OPERATION_PUT.getCode() )
                        {
                            put( in, out );
                        }
                        else if ( operationCode == OPERATION_GET.getCode() )
                        {
                            get( in, out );
                        }
                        else if ( operationCode == OPERATION_REMOVE.getCode() )
                        {
                            remove( in, out );
                        }
                        else if ( operationCode == OPERATION_LIST.getCode() )
                        {
                            list( out );
                        }
                        else if ( operationCode == OPERATION_DISCONNECT.getCode() )
                        {
                            System.out.println( "connection closed." );
                            clientConnected = false;
                        }

                        // Short delay to avoid busy-waiting while wait for the next incoming request
                        Thread.sleep( 500 );
                    }
                }
            }
        }
    }

    private static void put ( InputStream in,
                              OutputStream out )
            throws IOException
    {
        byte[] request;

        // Get the size of the key
        request = new byte[BYTES_PER_INTEGER];
        in.read( request );
        int sizeOfKey = ByteBuffer.wrap( request ).getInt();

        // Get the key
        request = new byte[sizeOfKey];
        in.read( request );
        String key = new String( request, StandardCharsets.UTF_8 );

        ByteBuffer buffer = ByteBuffer.allocate( BYTES_PER_INTEGER );

        // Check if the key already exists in the stores objects.
        if ( storedObjects.containsKey( key ) )
        {
            System.out.println( "key already exists" );
            buffer.putInt( KEY_ALREADY_EXIST ); // result code
        }
        else
        {
            // Get the size of the object
            request = new byte[BYTES_PER_INTEGER];
            in.read( request );
            int sizeOfObject = ByteBuffer.wrap( request ).getInt();

            // Get the object
            request = new byte[sizeOfObject];
            in.read( request );
            byte[] data = ByteBuffer.wrap( request ).array();

            System.out.println( "putting\"" + key + "\"" );

            // Add the new object into object store.
            storedObjects.put( key, data );

            buffer.putInt( SUCCESS );           // result code
        }

        // Send the response to client.
        out.write( buffer.array() );
        out.flush();
    }

    private static void get ( InputStream in,
                              OutputStream out )
            throws IOException
    {
        byte[] request;

        // Get the size of the key
        request = new byte[BYTES_PER_INTEGER];
        in.read( request );
        int sizeOfKey = ByteBuffer.wrap( request ).getInt();

        // Get the key
        request = new byte[sizeOfKey];
        in.read( request );
        String key = new String( request, StandardCharsets.UTF_8 );

        ByteBuffer buffer;

        // Check if the key already exists in the stores objects.
        if ( storedObjects.containsKey( key ) )
        {
            System.out.println( "getting\"" + key + "\"" );

            // Retrieve the object from object store.
            final byte[] data = storedObjects.get( key );

            // Send the object as a response to the client.
            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER + BYTES_PER_INTEGER + data.length );
            buffer.putInt( KEY_ALREADY_EXIST );  // Result code
            buffer.putInt( data.length );        // Size of the object
            buffer.put( data );                  // The object
        }
        else
        {
            System.out.println( "key does not exist" );
            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER );
            buffer.putInt( KEY_NOT_EXIST );      // Result code
        }

        // Send the response to client.
        out.write( buffer.array() );
        out.flush();
    }

    private static void remove ( InputStream in,
                                 OutputStream out )
            throws IOException
    {
        byte[] request;

        // Get the size of the key
        request = new byte[BYTES_PER_INTEGER];
        in.read( request );
        int sizeOfKey = ByteBuffer.wrap( request ).getInt();

        // Get the key
        request = new byte[sizeOfKey];
        in.read( request );
        String key = new String( request, StandardCharsets.UTF_8 );

        ByteBuffer buffer = ByteBuffer.allocate( BYTES_PER_INTEGER );

        // Check if the key already exists in the stores objects.
        if ( storedObjects.containsKey( key ) )
        {
            System.out.println( "removing\"" + key + "\"" );

            // Remove the object from object store.
            storedObjects.remove( key );

            buffer.putInt( SUCCESS );        // Result code
        }
        else
        {
            System.out.println( "key does not exist" );
            buffer.putInt( KEY_NOT_EXIST );  // Result code
        }

        // Send the response to client.
        out.write( buffer.array() );
        out.flush();
    }

    private static void list ( OutputStream out )
            throws IOException
    {
        ByteBuffer buffer;

        // Check if the object store is empty.
        if ( storedObjects.isEmpty() )
        {
            System.out.println( "no key exists" );
            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER );
            buffer.putInt( 0 );      // no item exists in the object store
        }
        else
        {
            // Retrieve the keys in the object store.
            String[] keys = storedObjects.keySet().toArray( new String[0] );

            // Convert the String[] into byte[]
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for ( String key : keys )
            {
                byte[] bytes = (key + KEY_SEPARATOR).getBytes();
                outputStream.write( bytes, 0, bytes.length );
            }
            byte[] data = outputStream.toByteArray();

            System.out.println( "sending list of keys" );

            // Send the object as a response to the client.
            buffer = ByteBuffer.allocate( BYTES_PER_INTEGER + data.length );
            buffer.putInt( data.length );        // Size of the object
            buffer.put( data );                  // The object
        }

        // Send the response to client.
        out.write( buffer.array() );
        out.flush();
    }

    public static void stopServer ()
    {
        running = false;
    }
}
