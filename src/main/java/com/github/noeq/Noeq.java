package com.github.noeq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Noeq
{

    private final String       token;
    private final List<Server> servers = new ArrayList<Server>();

    private Socket             conn;

    private static class Server
    {
        public final String address;
        public final int    port;

        //        public Server(String address, int port)
        //        {
        //            this.address = address;
        //            this.port = port;
        //        }

        public Server(String pair)
        {
            String[] split = pair.split(":");
            this.address = split[0];
            this.port = Integer.parseInt(split[1]);
        }
    }

    public Noeq(String token, String... addresses)
    {
        if (token == null)
        {
            this.token = "";
        }
        else
        {
            this.token = token;
        }
        for (String addr : addresses)
        {
            try
            {
                Server srv = new Server(addr);
                servers.add(srv);
            }
            catch (Exception e)
            {
                // Something...
            }
        }
    }

    public void connect() throws UnknownHostException, IOException
    {
        final int n = (int) (Math.random() * ((servers.size()) + 1));
        final Server srv = servers.get(n);
        conn = new Socket(srv.address, srv.port);
        auth();
    }

    public void disconnect() throws IOException
    {
        conn.shutdownInput();
        conn.shutdownOutput();
        conn.close();
    }

    private void auth() throws IOException
    {
        if (token.length() > 0)
        {
            final PrintWriter out = new PrintWriter(conn.getOutputStream(), true);
            out.print(String.format("\000%c%s", token.length(), token));
        }
    }

    public synchronized long[] get(int num) throws IOException
    {
        if (num < 1)
        {
            num = 1;
        }
        if (num > 255)
        {
            throw new IllegalArgumentException("requested more than 255 ids");
        }
        //System.out.println("Get " + num + " ids");
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
            //System.out.println(id);
            idList[i] = id;
        }
        return idList;
    }

    public long getOne() throws IOException
    {
        return get(1)[0];
    }

}
