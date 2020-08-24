package datawave.data.normalizer;

import com.google.common.collect.Lists;
import datawave.data.type.util.Geometry;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomNameIndex;

import java.util.List;

/**
 * A normalizer that, given a parseable geometry string representing an arbitrary geometry, will perform GeoWave indexing with a multi-tiered spatial geowave
 * index configuration
 *
 */
public class GeometryNormalizer extends AbstractGeometryNormalizer<Geometry,org.locationtech.jts.geom.Geometry> implements OneToManyNormalizer<Geometry> {
    private static final long serialVersionUID = 171360806347433135L;
    
    // @formatter:off
    public static final NumericIndexStrategy indexStrategy = TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
            new NumericDimensionDefinition[]{
                    new LongitudeDefinition(),
                    new LatitudeDefinition(
                            true)
                    // just use the same range for latitude to make square sfc values in
                    // decimal degrees (EPSG:4326)
            },
            new int[]{
                    LONGITUDE_BITS,
                    LATITUDE_BITS
            },
            SFCFactory.SFCType.HILBERT);
    // @formatter:on
    
    public static final Index index = new CustomNameIndex(indexStrategy, null, "geometryIndex");
    
    protected NumericIndexStrategy getIndexStrategy() {
        return GeometryNormalizer.indexStrategy;
    }
    
    protected Index getIndex() {
        return index;
    }
    
    @Override
    public List<String> normalizeToMany(String geoString) throws IllegalArgumentException {
        return normalizeDelegateTypeToMany(createDatawaveGeometry(parseGeometry(geoString)));
    }
    
    @Override
    public List<String> normalizeDelegateTypeToMany(Geometry geometry) {
        List<String> list = Lists.newArrayList();
        for (byte[] one : getIndicesFromGeometry(geometry)) {
            list.add(getEncodedStringFromIndexBytes(one));
        }
        return list;
    }
    
    protected datawave.data.type.util.Geometry createDatawaveGeometry(org.locationtech.jts.geom.Geometry geometry) {
        return new datawave.data.type.util.Geometry(geometry);
    }
}
