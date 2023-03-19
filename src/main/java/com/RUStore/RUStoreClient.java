package com.RUStore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

import static com.RUStore.Constant.BYTES_PER_INTEGER;
import static com.RUStore.Constant.KEY_ALREADY_EXIST;
import static com.RUStore.Constant.KEY_NOT_EXIST;
import static com.RUStore.Constant.SUCCESS;
import static com.RUStore.OperationCode.OPERATION_DISCONNECT;
import static com.RUStore.OperationCode.OPERATION_GET;
import static com.RUStore.OperationCode.OPERATION_LIST;
import static com.RUStore.OperationCode.OPERATION_PUT;
import static com.RUStore.OperationCode.OPERATION_REMOVE;

public class RUStoreClient
{
    private Socket socket;
    private final String host;
    private final int port;

    private OutputStream out;
    private InputStream in;

    /**
     RUStoreClient Constructor, initializes default values
     for class members

     @param host host url
     @param port port number
     */
    public RUStoreClient ( final String host,
                           final int port )
    {
        this.host = host;
        this.port = port;
    }

    /**
     Opens a socket and establish a connection to the object store server
     running on a given host and port.

     @return n/a, however throw an exception if any issues occur
     */
    public void connect ()
            throws IOException
    {
        this.socket = new Socket();
        this.socket.connect( new InetSocketAddress( host, port ) );
        this.out = new BufferedOutputStream( socket.getOutputStream() );
        this.in = new BufferedInputStream( socket.getInputStream() );
    }

    /**
     Sends an arbitrary data object to the object store server. If an
     object with the same key already exists, the object should NOT be
     overwritten

     @param key key to be used as the unique identifier for the object
     @param data byte array representing arbitrary data object
     @return 0 upon success
     1 if key already exists
     Throw an exception otherwise
     */
    public int put ( final String key,
                     final byte[] data )
            throws IOException, InterruptedException
    {
        ByteBuffer buffer = ByteBuffer.allocate( OPERATION_PUT.getByteLength() + BYTES_PER_INTEGER + key.getBytes().length + BYTES_PER_INTEGER + data.length );
        buffer.putInt( OPERATION_PUT.getCode() );  // Operation code
        buffer.putInt( key.getBytes().length );    // Size of the key
        buffer.put( key.getBytes() );              // The key
        buffer.putInt( data.length );              // Size of the object
        buffer.put( data );                        // The object

        // Send the data object to the object store server.
        out.write( buffer.array() );
        out.flush();

        // Wait for the server to process the data before reading the response.
        Thread.sleep( 2000 );

        // Get the result code from the object store server.
        byte[] response = new byte[BYTES_PER_INTEGER];
        in.read( response, 0, BYTES_PER_INTEGER );

        return ByteBuffer.wrap( response ).getInt(); // either SUCCESS or KEY_ALREADY_EXIST
    }

    /**
     Sends an arbitrary data object to the object store server. If an
     object with the same key already exists, the object should NOT
     be overwritten.

     @param key key to be used as the unique identifier for the object
     @param file_path path of file data to transfer
     @return 0 upon success
     1 if key already exists
     Throw an exception otherwise
     */
    public int put ( final String key,
                     final String file_path )
            throws IOException, InterruptedException
    {
        // Read the data object from the file.
        File file = new File( file_path );
        byte[] data = new byte[( int ) file.length()];
        try ( FileInputStream in = new FileInputStream( file ) )
        {
            in.read( data );
        }

        // Send the data object to the object store server.
        return put( key, data );
    }

    /**
     Downloads arbitrary data object associated with a given key
     from the object store server.

     @param key key associated with the object
     @return object data as a byte array, null if key doesn't exist.
     Throw an exception if any other issues occur.
     */
    public byte[] get ( final String key )
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( OPERATION_GET.getByteLength() + BYTES_PER_INTEGER + key.getBytes().length );
        buffer.putInt( OPERATION_GET.getCode() );  // Operation code
        buffer.putInt( key.getBytes().length );    // Size of the key
        buffer.put( key.getBytes() );              // The key

        // Send the data object to the object store server.
        out.write( buffer.array() );
        out.flush();

        // Get the response
        byte[] response = new byte[BYTES_PER_INTEGER];
        in.read( response );
        int resultCode = ByteBuffer.wrap( response ).getInt();

        if ( resultCode == KEY_ALREADY_EXIST )
        {
            // Get the object's length
            response = new byte[BYTES_PER_INTEGER];
            in.read( response );
            int objectLength = ByteBuffer.wrap( response ).getInt();

            // Get the object.
            response = new byte[objectLength];
            in.read( response );

            return ByteBuffer.wrap( response ).array();
        }
        else
        {
            return null;
        }
    }

    /**
     Downloads arbitrary data object associated with a given key
     from the object store server and places it in a file.

     @param key key associated with the object
     @param file_path output file path
     @return 0 upon success
     1 if key doesn't exist
     Throw an exception otherwise
     */
    public int get ( final String key,
                     final String file_path )
            throws IOException
    {
        byte[] data = get( key );

        if ( data == null )
        {
            return KEY_NOT_EXIST;
        }
        else
        {
            File file = new File( file_path );
            try ( FileOutputStream out = new FileOutputStream( file ) )
            {
                out.write( data );
                return SUCCESS;
            }
        }
    }

    /**
     Removes data object associated with a given key
     from the object store server. Note: No need to download the data object,
     simply invoke the object store server to remove object on server side

     @param key key associated with the object
     @return 0 upon success
     1 if key doesn't exist
     Throw an exception otherwise
     */
    public int remove ( final String key )
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( OPERATION_REMOVE.getByteLength() + BYTES_PER_INTEGER + key.getBytes().length );
        buffer.putInt( OPERATION_REMOVE.getCode() ); // Operation code
        buffer.putInt( key.getBytes().length );      // Size of the key
        buffer.put( key.getBytes() );                // The key

        // Send the command to the object store server.
        out.write( buffer.array() );
        out.flush();

        // Get the response
        byte[] response = new byte[BYTES_PER_INTEGER];
        in.read( response );

        return ByteBuffer.wrap( response ).getInt();  // either SUCCESS or KEY_NOT_EXIST
    }

    /**
     Retrieves of list of object keys from the object store server

     @return Array of keys as string array, null if there are no keys.
     Throw an exception if any other issues occur.
     */
    public String[] list ()
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( OPERATION_LIST.getByteLength() );
        buffer.putInt( OPERATION_LIST.getCode() ); // Operation code

        // Send the data object to the object store server.
        out.write( buffer.array() );
        out.flush();

        // Get the response
        byte[] response = new byte[BYTES_PER_INTEGER];
        in.read( response );
        int listSize = ByteBuffer.wrap( response ).getInt();

        if ( listSize == 0 )
        {
            return null;
        }

        // Get the array.
        response = new byte[listSize];
        in.read( response );

        // Convert byte[] into String[]
        ByteArrayInputStream inputStream = new ByteArrayInputStream( response );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        int len;
        byte[] bytes = new byte[1024];
        while ( (len = inputStream.read( bytes, 0, bytes.length )) != -1 )
        {
            outputStream.write( bytes, 0, len );
        }

        return outputStream.toString().split( "\\|" );
    }

    /**
     Signals to server to close connection before closes
     the client socket.

     @return n/a, however throw an exception if any issues occur
     */
    public void disconnect ()
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( OPERATION_DISCONNECT.getByteLength() );
        buffer.putInt( OPERATION_DISCONNECT.getCode() ); // Operation code

        // Send the data object to the object store server.
        out.write( buffer.array() );
        out.flush();

        in.close();
        out.close();
        socket.close();
    }
}