package eu.cymo.avro_composer.adapter.kafka.stream.topology;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.test.context.TestContextAnnotationUtils;
import org.springframework.test.context.web.WebMergedContextConfiguration;

public class TopologyTestContextBootstrapper extends SpringBootTestContextBootstrapper {

    @Override
    protected MergedContextConfiguration processMergedContextConfiguration(MergedContextConfiguration mergedConfig) {
        var autoConfigureMockMvc = TestContextAnnotationUtils.findMergedAnnotation(mergedConfig.getTestClass(), AutoConfigureMockMvc.class);
        var processedMergedConfiguration = super.processMergedContextConfiguration(mergedConfig);
        if (autoConfigureMockMvc != null) {
            return new WebMergedContextConfiguration(processedMergedConfiguration, determineResourceBasePath(mergedConfig));
        } else {
            return processedMergedConfiguration;
        }
    }

    @Override
    protected String[] getProperties(Class<?> testClass) {
        TopologyTest topologyTest = TestContextAnnotationUtils.findMergedAnnotation(testClass, TopologyTest.class);
        return (topologyTest != null) ? topologyTest.properties() : null;
    }
    
}
