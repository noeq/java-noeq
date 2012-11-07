package com.github.noeq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

public class Noeq
{

    /**
     * We want a relatively fast fail before moving on to the next server. 250ms is probably slower than we want, but a
     * good place to start until we have better benchmarks.
     */
    private static final int              DEFAULT_TIMEOUT         = 250;

    /**
     * With default timeout of 250ms, we probably don't want the total connection time to exceed one second.
     */
    private static final int              MAX_CONNECTION_ATTEMPTS = 4;

    private final String                  token;
    private final int                     timeout;
    private final List<InetSocketAddress> servers                 = new ArrayList<InetSocketAddress>();
    private final Socket                  conn                    = new Socket();

    /**
     * Initialize a Noeq connection to a list of comma-delimited host:port adresses
     * 
     * @param token
     * @param addresses
     */
    public Noeq(String token, String addresses)
    {
        this(token, addresses, DEFAULT_TIMEOUT);
    }

    /**
     * Initialize a Noeq connection to a list of comma-delimited host:port adresses
     * 
     * @param token
     * @param addresses
     */
    public Noeq(String token, String addresses, int timeout)
    {
        this(token, timeout, addresses.split(","));
    }

    /**
     * Initialize a Noeq connection to an array of host:port address strings. Invalid or un-resolvable hosts will be
     * ignored.
     * 
     * @param token
     * @param addresses
     */
    public Noeq(String token, int timeout, String... addresses)
    {
        if (token == null)
        {
            this.token = "";
        }
        else
        {
            this.token = token;
        }
        this.timeout = timeout < 1 ? DEFAULT_TIMEOUT : timeout;
        for (String addr : addresses)
        {
            try
            {
                String[] split = addr.split(":", 2);
                String host = split[0];
                final int port;
                if (split.length == 2)
                {
                    port = Integer.parseInt(split[1]);
                }
                else
                {
                    port = 4444;
                }
                InetSocketAddress address = new InetSocketAddress(host, port);
                if (address.isUnresolved())
                {
                    // TODO: Something...
                }
                else
                {
                    servers.add(address);
                }
            }
            catch (Exception e)
            {
                // TODO: Something...
            }
        }
        if (servers.isEmpty())
        {
            throw new IllegalArgumentException("No valid server addresses were provided");
        }
    }

    public boolean isConnected()
    {
        return conn.isConnected();
    }

    /**
     * Connect to a random server from the list. If it cannot be reached within the requested timeout, try the next
     * server in the list until a connection is successful or MAX_CONNECTION_ATTEMPTS is reached.
     * 
     * @throws IOException
     * @throws SocketTimeoutException
     *             if MAX_CONNECTION_ATTEMPTS connection attempts time out.
     */
    public void connect() throws IOException
    {
        if (conn.isConnected())
        {
            return;
        }
        final int size = servers.size();
        int tries = Math.min(MAX_CONNECTION_ATTEMPTS, size);
        int n = (int) (Math.random() * size); // Don't bother to add 1 for the random() because of zero-index array
        while (true)
        {
            try
            {
                final InetSocketAddress addr = servers.get(n);
                //System.out.println("Connect to " + addr);
                conn.connect(addr, this.timeout);
                break;
            }
            catch (SocketTimeoutException e)
            {
                // Too many connection attempts?
                tries--;
                if (tries < 1)
                {
                    throw e;
                }
                // Just move to the next server in the list, or loop to the start
                if (n < size)
                {
                    n++;
                }
                else
                {
                    n = 0;
                }
            }
        }
        auth();
    }

    /**
     * Disconnect and clean up the connection
     * 
     * @throws IOException
     */
    public void disconnect() throws IOException
    {
        if (!isConnected())
        {
            return;
        }
        // Split up the exception catches.  Messy but it makes sure that
        // each of these gets a chance to run.
        try
        {
            conn.shutdownInput();
        }
        catch (IOException e)
        {
        }
        try
        {
            conn.shutdownOutput();
        }
        catch (IOException e)
        {
        }
        // We can allow this to throw an exception
        conn.close();
    }

    /**
     * Send the auth token
     * 
     * @throws IOException
     */
    private void auth() throws IOException
    {
        if (token.length() > 0)
        {
            //System.out.println("AUTH: " + token);
            final PrintWriter out = new PrintWriter(conn.getOutputStream(), true);
            out.print(String.format("\000%c%s", token.length(), token));
            out.flush();
        }
    }

    /**
     * Get the requested number of id values from noeqd
     * 
     * @param num
     *            number of id values to request
     * @return long[] of id values, or null if there was a problem
     */
    public synchronized long[] get(int num)
    {
        if (num < 1)
        {
            num = 1;
        }
        else if (num > 255)
        {
            throw new IllegalArgumentException("requested more than 255 ids");
        }
        //System.out.println("Get " + num + " ids");
        try
        {
            // Make sure we're connected to the server
            connect();
            // Request the proper number of IDs
            final DataOutputStream out = new DataOutputStream(conn.getOutputStream());
            out.writeByte(num);
            out.flush();
            // Read the results
            final DataInputStream in = new DataInputStream(conn.getInputStream());
            final long[] idList = new long[num];
            for (short i = 0; i < num; i++)
            {
                long id = in.readLong();
                //System.out.println("Receive id " + id);
                idList[i] = id;
            }
            return idList;
        }
        catch (IOException e)
        {
            // e.printStackTrace();
            try
            {
                disconnect();
            }
            catch (IOException e2)
            {
                // Ignore
            }
            return null;
        }
    }

    /**
     * Get one id from noeqd
     * 
     * @return a single id value
     */
    public long getOne()
    {
        long[] idList = get(1);
        return (idList == null) ? null : idList[0];
    }

}
