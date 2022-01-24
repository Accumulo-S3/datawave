package datawave.ingest.data.config.ingest;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VirtualFieldTest {
    
    protected Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
    
    @Before
    public void setup() {
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value1", "group1", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value2", "group2", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value3", "group3", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value4", "group4", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value5", "group5", "subgroup1"));
        
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value1", "group1", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value2", "group2", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value3", "group3", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value4", "group4", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value5", "group5", "subgroup1"));
        
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value1"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value2"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value3"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value4"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value5"));
        
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value1"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value2"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value3"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value4"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value5"));
        
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value1", "group1", "subgroup1"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value2", "group2", "subgroup1"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value3", "group3", "subgroup1"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value4"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value5"));
        
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value1"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value2"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value3", "group3", "subgroup1"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value4", "group4", "subgroup1"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value5", "group5", "subgroup1"));
    }
    
    protected VirtualFieldIngestHelper getHelper(VirtualIngest.GroupingPolicy policy) {
        VirtualFieldIngestHelper helper = new VirtualFieldIngestHelper(new Type("test", null, null, null, 1, null));
        Configuration config = new Configuration();
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_NAMES, "group1group2,group1ungroup1,ungroup1group1,ungroup1ungroup2,partial1partial2");
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_MEMBERS,
                        "GROUPED_1.GROUPED_2,GROUPED_1.UNGROUPED_1,UNGROUPED_1.GROUPED_1,UNGROUPED_1.UNGROUPED_2,PARTIAL_1.PARTIAL_2");
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_GROUPING_POLICY, policy.name() + ',' + policy.name() + ',' + policy.name() + ',' + policy.name() + ','
                        + policy.name());
        helper.setup(config);
        return helper;
    }
    
    @Test
    public void testSameGroupOnlyVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getHelper(VirtualIngest.GroupingPolicy.SAME_GROUP_ONLY);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);
        
        assertEquals(3, virtualFields.keySet().size());
        assertEquals(25, virtualFields.get("ungroup1ungroup2").size());
        assertEquals(5, virtualFields.get("group1group2").size());
        assertEquals(5, virtualFields.get("partial1partial2").size());
    }
    
    @Test
    public void testGroupedWithNonGroupedVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getHelper(VirtualIngest.GroupingPolicy.GROUPED_WITH_NON_GROUPED);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);
        
        assertEquals(5, virtualFields.keySet().size());
        assertEquals(25, virtualFields.get("ungroup1ungroup2").size());
        assertEquals(5, virtualFields.get("group1group2").size());
        assertEquals(25, virtualFields.get("ungroup1group1").size());
        assertEquals(25, virtualFields.get("group1ungroup1").size());
        assertEquals(17, virtualFields.get("partial1partial2").size());
    }
    
    @Test
    public void testIgnoreGroupsVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getHelper(VirtualIngest.GroupingPolicy.IGNORE_GROUPS);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);
        
        assertEquals(5, virtualFields.keySet().size());
        assertEquals(25, virtualFields.get("ungroup1ungroup2").size());
        assertEquals(25, virtualFields.get("group1group2").size());
        assertEquals(25, virtualFields.get("ungroup1group1").size());
        assertEquals(25, virtualFields.get("group1ungroup1").size());
        assertEquals(25, virtualFields.get("partial1partial2").size());
    }
    
}
