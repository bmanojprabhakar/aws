package io.backspace;

import io.backspace.util.S3Util;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        S3Util s3Util = new S3Util();
        System.out.println( "Starting Auto PortIn!" );
        s3Util.listObjects();
    }
}
