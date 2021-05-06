package datawave.iterators.filter;

import datawave.iterators.filter.ageoff.FilterOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class EdgeColumnQualifierTokenFilter extends TokenizingFilterBase {
    
    private static final byte[] CV_DELIMITERS = "/".getBytes();
    
    @Override
    public byte[] getKeyField(Key k, Value V) {
        return k.getColumnQualifierData().getBackingArray();
    }
    
    @Override
    public byte[] getDelimiters(FilterOptions options) {
        return CV_DELIMITERS;
    }
}