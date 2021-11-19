package datawave.ingest.mapreduce;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

public interface FieldSalvager {
    Multimap<String,? extends NormalizedContentInterface> getSalvageableEventFields(RawRecordContainer value);
}
