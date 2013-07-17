package com.couchbase.plugin.client;


import org.junit.*;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AutomaticVersioningBasicTest {

    private static CouchbaseClientWithVersioning client = null;

    public AutomaticVersioningBasicTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {

        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create("http://127.0.0.1:8091/pools"));

        try {
            client = new CouchbaseClientWithVersioning(uris, "default", "");
			client.setAutomaticVersionning(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @After
    public void tearDown() {

        if (client != null) {
            client.shutdown(5, TimeUnit.SECONDS);
        }

    }


    @Test
    public void testOfSetOperation() {
        try {

            String key001 = "key001";
            String v1 = "{\"name\":\"this is a test\", \"num\":1}";
            String v2 = "{\"name\":\"this is a test\", \"num\":2}";
            String v3 = "{\"name\":\"this is a test\", \"num\":3}";

            // insert new value and test it
            client.set(key001, v1);
            assertEquals(v1, client.get(key001));
            assertEquals(null, client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key001, v2);
            assertEquals(v2, client.get(key001));
            assertEquals(v1, client.get(key001, 1));
            assertEquals("1", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key001, v3);
            assertEquals(v3, client.get(key001));
            assertEquals(v1, client.get(key001, 1));
            assertEquals(v2, client.get(key001, 2));
            assertEquals("2", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // delete one version
            client.deleteVersion(key001, 1);
            assertEquals(v3, client.get(key001));
            assertEquals(null, client.get(key001, 1));
            assertEquals(v2, client.get(key001, 2));
            assertEquals("2", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));


            // delete the main document (and all version
            client.delete(key001);
            assertEquals(null, client.get(key001));
            assertEquals(null, client.get(key001, 1));
            assertEquals(null, client.get(key001, 2));
            assertEquals(null, client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));


            // insert new value and test it
            client.set(key001, v1);
            assertEquals(v1, client.get(key001));
            assertEquals(null, client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key001, v2);
            assertEquals(v2, client.get(key001));
            assertEquals(v1, client.get(key001, 1));
            assertEquals("1", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key001, v3);
            assertEquals(v3, client.get(key001));
            assertEquals(v1, client.get(key001, 1));
            assertEquals(v2, client.get(key001, 2));
            assertEquals("2", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // delete one version
            client.deleteVersion(key001, 1);
            assertEquals(v3, client.get(key001));
            assertEquals(null, client.get(key001, 1));
            assertEquals(v2, client.get(key001, 2));
            assertEquals("2", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));


            // delete the main document (and all version
            client.delete(key001);
            assertEquals(null, client.get(key001));
            assertEquals(null, client.get(key001, 1));
            assertEquals(null, client.get(key001, 2));
            assertEquals(null, client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }




}
