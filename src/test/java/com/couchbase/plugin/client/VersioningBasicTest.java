package com.couchbase.plugin.client;


import org.junit.*;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class VersioningBasicTest {

    private static CouchbaseClientWithVersioning client = null;

    public VersioningBasicTest() {
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
    public void testOfSetOperationWithVersion() {
        try {

            String key001 = "key001";
            String v1 = "{\"name\":\"this is a test\", \"num\":1}";
            String v2 = "{\"name\":\"this is a test\", \"num\":2}";
            String v3 = "{\"name\":\"this is a test\", \"num\":3}";

            // insert new value and test it
            client.set(key001, v1, true);
            assertEquals(v1, client.get(key001));
            assertEquals(null, client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key001, v2, true);
            assertEquals(v2, client.get(key001));
            assertEquals(v1, client.get(key001, 1));
            assertEquals("1", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key001, v3, true);
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
            client.set(key001, v2, true);
            assertEquals(v2, client.get(key001));
            assertEquals(v1, client.get(key001, 1));
            assertEquals("1", client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key001, v3, true);
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


    @Test
    public void testOfReplaceOperationWithVersion() {
        try {

            String key002 = "key002";
            String v1 = "{\"name\":\"this is a test\", \"num\":1}";
            String v2 = "{\"name\":\"this is a test\", \"num\":2}";
            String v3 = "{\"name\":\"this is a test\", \"num\":3}";

            // insert new value and test it
            client.set(key002, v1);
            assertEquals(v1, client.get(key002));
            assertEquals(null, client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.replace(key002, v2);
            assertEquals(v2, client.get(key002));
            assertEquals(v1, client.get(key002, 1));
            assertEquals("1", client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.replace(key002, v3);
            assertEquals(v3, client.get(key002));
            assertEquals(v1, client.get(key002, 1));
            assertEquals(v2, client.get(key002, 2));
            assertEquals("2", client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // delete one version
            client.deleteVersion(key002, 1);
            assertEquals(v3, client.get(key002));
            assertEquals(null, client.get(key002, 1));
            assertEquals(v2, client.get(key002, 2));
            assertEquals("2", client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));


            // delete the main document (and all version
            client.delete(key002);
            assertEquals(null, client.get(key002));
            assertEquals(null, client.get(key002, 1));
            assertEquals(null, client.get(key002, 2));
            assertEquals(null, client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));


            // insert new value and test it
            client.set(key002, v1);
            assertEquals(v1, client.get(key002));
            assertEquals(null, client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key002, v2, true);
            assertEquals(v2, client.get(key002));
            assertEquals(v1, client.get(key002, 1));
            assertEquals("1", client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // update the value and create a version
            client.set(key002, v3, true);
            assertEquals(v3, client.get(key002));
            assertEquals(v1, client.get(key002, 1));
            assertEquals(v2, client.get(key002, 2));
            assertEquals("2", client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

            // delete one version
            client.deleteVersion(key002, 1);
            assertEquals(v3, client.get(key002));
            assertEquals(null, client.get(key002, 1));
            assertEquals(v2, client.get(key002, 2));
            assertEquals("2", client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));


            // delete the main document (and all version
            client.delete(key002);
            assertEquals(null, client.get(key002));
            assertEquals(null, client.get(key002, 1));
            assertEquals(null, client.get(key002, 2));
            assertEquals(null, client.get(key002 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }


    @Test
    public void testOfSetOperationWithoutVersion() {
        String key003 = "key003";
        String v1 = "{\"name\":\"this is a test\", \"num\":1}";
        String v2 = "{\"name\":\"this is a test\", \"num\":2}";

        client.set( key003, v1 );
        client.set( key003, v2 );

        // only 1 key no version should exist
        assertEquals(v2, client.get(key003));
        assertEquals(null, client.get(key003, 1) );
        assertEquals(null, client.get(key003 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

        client.delete(key003);
        assertEquals(null, client.get(key003) );
    }



	@Test
	public void testOfSetNoVersion() {
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
			assertEquals(null, client.get(key001, 1));
			assertEquals(null, client.get(key001 + CouchbaseClientWithVersioning.VERSION_COUNTER_SUFFIX));

			// delete the main document (and all version
			client.delete(key001);
			assertEquals(null, client.get(key001));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}


}
