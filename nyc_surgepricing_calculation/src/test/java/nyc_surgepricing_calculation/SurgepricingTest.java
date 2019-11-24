/**
 * 
 */
package nyc_surgepricing_calculation;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nyc.surgepricing.calculator.SurgepricingCalculator;
import com.nyc.surgepricing.constants.SurgepricingConstants;
import com.nyc.surgepricing.utils.JavaSparksessionSingleton;

/**
 * @author AMITH DAS
 *
 */
public class SurgepricingTest implements SurgepricingConstants {
	private static SparkSession sparkSession = null;
	private static Dataset<Row> yelloCabDF = null;
	private static Dataset<Row> weatherDF = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		sparkSession = JavaSparksessionSingleton.getInstance(sparkSession);
		yelloCabDF = SurgepricingCalculator.deriveTripAttributes(sparkSession,
				SurgepricingCalculator.getYellCabDF(sparkSession));
		weatherDF = SurgepricingCalculator.fetchWeatherDataSet(sparkSession);
	}

	@Test
	public void surgePricingTest() {

		Dataset<Row> demandSupplyDF = SurgepricingCalculator.deriveSupplyDemandRatio(sparkSession, yelloCabDF);
		Dataset<Row> surgeWithWeatherEffDF = SurgepricingCalculator.corelateWeatherEffect(sparkSession, demandSupplyDF,
				weatherDF);
		assertNotEquals(surgeWithWeatherEffDF.count(), 37748);
	}

	@Test
	public void trafficCongestionTest() {
		Dataset<Row> trafficCongestionDF = SurgepricingCalculator.calculateTrafficCongestion(sparkSession, yelloCabDF);
		Dataset<Row> trafficWithWeatherEffDF = SurgepricingCalculator.corelateWeatherEffect(sparkSession,
				trafficCongestionDF, weatherDF);
		assertNotEquals(trafficWithWeatherEffDF.count(), 38512);
	}

	/**
	 * 
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

}
