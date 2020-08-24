package datawave.data.normalizer;

import datawave.data.type.util.Point;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomNameIndex;

/**
 * A normalizer that, given a parseable geometry string representing a point geometry will perform GeoWave indexing with a single-tier spatial geowave index
 * configuration
 *
 */
public class PointNormalizer extends AbstractGeometryNormalizer<Point,org.locationtech.jts.geom.Point> {
    private static final long serialVersionUID = 171360806347433135L;
    
    // @formatter:off
    public static final NumericIndexStrategy indexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
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
    
    public static final Index index = new CustomNameIndex(indexStrategy, null, "pointIndex");
    
    protected NumericIndexStrategy getIndexStrategy() {
        return PointNormalizer.indexStrategy;
    }
    
    protected Index getIndex() {
        return index;
    }
    
    protected Point createDatawaveGeometry(org.locationtech.jts.geom.Point geometry) {
        return new Point(geometry);
    }
}
