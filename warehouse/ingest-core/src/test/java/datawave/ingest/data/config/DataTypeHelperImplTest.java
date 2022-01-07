package datawave.ingest.data.config;

import datawave.policy.IngestPolicyEnforcer;
import datawave.util.IngestTestSetup;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

import static org.hamcrest.core.Is.is;

public class DataTypeHelperImplTest {
    
    private Configuration conf;
    
    @Before
    public void setup() {
        conf = new Configuration();
        conf.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConfig() {
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        IngestTestSetup.resetTypeRegistry(conf);
        helper.setup(conf);
    }
    
    @Test
    public void testValidConfig() throws Exception {
        InputStream configStream = getClass().getResourceAsStream("/fake-datatype-config.xml");
        Assert.assertNotNull(configStream);
        conf.addResource(configStream);
        Assert.assertThat(conf.get("data.name"), is("fake"));
        IngestTestSetup.resetTypeRegistry(conf);
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        helper.setup(conf);
        
        Assert.assertTrue(helper.fieldsToDowncase.contains("md5"));
        Assert.assertTrue(helper.fieldsToDowncase.contains("sha1"));
        Assert.assertTrue(helper.fieldsToDowncase.contains("sha256"));
        
        Assert.assertEquals("abcde", helper.clean("MD5", "ABCDE"));
    }
    
    @Test
    public void testDowncaseFields() throws Exception {
        InputStream configStream = getClass().getResourceAsStream("/fake-datatype-config.xml");
        Assert.assertNotNull(configStream);
        conf.addResource(configStream);
        conf.set("fake" + DataTypeHelper.Properties.DOWNCASE_FIELDS, "one,two,three,FOUR");
        IngestTestSetup.resetTypeRegistry(conf);
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        helper.setup(conf);
        
        Assert.assertTrue(helper.fieldsToDowncase.contains("one"));
        Assert.assertTrue(helper.fieldsToDowncase.contains("two"));
        Assert.assertTrue(helper.fieldsToDowncase.contains("three"));
        
        Assert.assertEquals("abcde", helper.clean("THREE", "ABCDE"));
        Assert.assertEquals("abcde", helper.clean("FOUR", "ABCDE"));
    }
}
