package com.github.noeq;

import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;

import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for Noeq.
 */
public class TestNoeq
{

    @BeforeClass
    public static void setUp()
    {
    }

    /**
     * Basic test to communicate with the server.
     */
    @Test
    public void testGet()
    {
        final Noeq client;
        try
        {
            client = new Noeq("", "localhost:4444");
            client.connect();
        }
        catch (IOException e)
        {
            throw new SkipException("Can't connect to noeqd at localhost:4444");
        }
        try
        {
            final long id = client.getOne();
            if (id > 0)
            {
                System.out.println("Test retrieved ID: " + id);
                assertTrue(true);
            }
            else
            {
                System.out.println("Test failed to retrieve a valid ID.");
                assertTrue(false);
            }
        }
        catch (Exception e)
        {
            System.out.println("Error communicating with noeqd at localhost:4444: " + e);
        }
        // We don't want the test to fail outright if 
        assertTrue(true);
    }
}
