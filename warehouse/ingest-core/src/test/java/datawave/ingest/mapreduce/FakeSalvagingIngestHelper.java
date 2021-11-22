package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * This is a TEST-only implementation of the FieldSalvager interface.
 *
 * It will always throw an exception when getEventFields is called.
 *
 * Its implementation of getSalvageableEventFields will deserialized a Map&lt;String, String&gt;,
 * and for any of the
 */
public class FakeSalvagingIngestHelper extends ContentBaseIngestHelper implements FieldSalvager  {

    private String[] fieldsToSalvage;
    private static final Logger log = Logger.getLogger(FakeSalvagingIngestHelper.class);

    public void setFieldsToSalvage(String ... fieldsToSalvage) {
        this.fieldsToSalvage = fieldsToSalvage;
    }

    @Override
    public Multimap<String, ? extends NormalizedContentInterface> getSalvageableEventFields(RawRecordContainer value) {
        HashMultimap<String,NormalizedContentInterface> fields = HashMultimap.create();
        byte[] rawData = value.getRawData();

        try {
            ByteArrayInputStream bytArrayInputStream = new ByteArrayInputStream(rawData);
            ObjectInputStream objectInputStream = new ObjectInputStream(bytArrayInputStream);
            Map<String, String> deserializedData = (Map<String, String>) objectInputStream.readObject();
            for (String fieldToSalvage : fieldsToSalvage) {
                String fieldValue = deserializedData.get(fieldToSalvage);
                if (null != fieldValue) {
                    try {
                        fields.put(fieldToSalvage, new BaseNormalizedContent(fieldToSalvage, fieldValue));
                    } catch (Exception fieldException) {
                        log.debug("Exception seen for " + fieldToSalvage);
                        // skip this field and proceed to the next
                    }
                }
            }
        } catch (Exception e) {
            return fields;
        }
        return fields;
    }

    @Override
    public Multimap<String, NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        throw new RuntimeException("Simulated exception while getting event fields for value.");
    }
}
