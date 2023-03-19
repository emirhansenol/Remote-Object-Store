package com.RUStore;

public enum OperationCode
{
    /**
     Operation codes
     */
    OPERATION_PUT( 100, 4 ),
    OPERATION_GET( 200, 4 ),
    OPERATION_REMOVE( 300, 4 ),
    OPERATION_LIST( 400, 4 ),
    OPERATION_DISCONNECT( 500, 4 );

    private final int code;
    private final int byteLength;

    OperationCode ( final int code,
                    final int byteLength )
    {
        this.code = code;
        this.byteLength = byteLength;
    }

    public int getCode ()
    {
        return code;
    }

    public int getByteLength ()
    {
        return byteLength;
    }
}
