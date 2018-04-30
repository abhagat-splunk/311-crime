package praneeth.bdad;

import java.io.File;
import java.io.IOException;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.swing.data.JFileDataStoreChooser;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class PointInPolygon {
  private static SimpleFeatureCollection features;
  private static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(GeoTools
      .getDefaultHints());

  public static void main(String[] args)
      throws Exception {
    System.out.println(giveZip("43.0", "70.0"));
  }

  public static String giveZip(String latitude, String longitude)
      throws Exception {
    File file = getFile();
    FileDataStore store = FileDataStoreFinder.getDataStore(file);
    SimpleFeatureSource featureSource = store.getFeatureSource();

    PointInPolygon tester = new PointInPolygon();
    features = featureSource.getFeatures();
    GeometryFactory fac = new GeometryFactory();

    double lat = Double.parseDouble(latitude);
    double lon = Double.parseDouble(longitude);
    Point p = fac.createPoint(new Coordinate(lon, lat));
    return tester.isInShape(p);
  }

  private static String isInShape(Point p) throws Exception {
    CoordinateReferenceSystem sourceCRS = DefaultGeographicCRS.WGS84;
    CoordinateReferenceSystem layerCRS = features.getSchema().getCoordinateReferenceSystem();
    MathTransform transform= CRS.findMathTransform(sourceCRS, layerCRS,true);
    Point rp = (Point) JTS.transform(p, transform);
    System.out.println(p+" "+rp);

    Expression propertyName = ff.property(features.getSchema()
        .getGeometryDescriptor().getName());
    Filter filter = ff.contains(propertyName,
        ff.literal(rp));
    SimpleFeatureCollection sub = features.subCollection(filter);
    if (sub.size() == 1) {
      SimpleFeature s= sub.features().next();
      return (String) s.getAttribute("ZIPCODE");
    }
    throw new Exception();
  }

  private static File getFile() throws URISyntaxException, IOException {
    String ms = "zip53tnowg4nio4iht40gn";
    File parent = new File(System.getProperty("java.io.tmpdir"));
    File tempShp = new File(parent, ms + ".shp");
    if (!tempShp.exists()) {
      InputStream is = PointInPolygon.class.getResourceAsStream("ZIP_CODE_040114.shp");
      FileUtils.copyInputStreamToFile(is, tempShp);
      is.close();
    }

    File tempShx = new File(parent, ms + ".shx");
    if (!tempShx.exists()) {
      InputStream is = PointInPolygon.class.getResourceAsStream("ZIP_CODE_040114.shx");
      FileUtils.copyInputStreamToFile(is, tempShx);
      is.close();
    }

    File tempDbf = new File(parent, ms + ".dbf");
    if (!tempDbf.exists()) {
      InputStream is = PointInPolygon.class.getResourceAsStream("ZIP_CODE_040114.dbf");
      FileUtils.copyInputStreamToFile(is, tempDbf);
      is.close();
    }

    File tempPrj = new File(parent, ms + ".prj");
    if (!tempPrj.exists()) {
      InputStream is = PointInPolygon.class.getResourceAsStream("ZIP_CODE_040114.prj");
      FileUtils.copyInputStreamToFile(is, tempPrj);
      is.close();
    }

    return tempShp;
  }
}