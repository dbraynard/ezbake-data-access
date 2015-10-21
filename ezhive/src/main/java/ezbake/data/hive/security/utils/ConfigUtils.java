package ezbake.data.hive.security.utils;

import ezbake.configuration.EzConfigurationLoader;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.OpenShiftConfigurationLoader;
import ezbake.configuration.PropertiesConfigurationLoader;
import java.nio.file.Path;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConfigUtils {
    /**
     * This method sets properties in the same way as a ThriftRunner,
     * with the exception that this cannot read anything from the
     * commandline. Instead, additional paths are passed and used to
     * read additional configuration. These should be used, because
     * the security ID and some other settings are
     * application-specific.
     * 
     * @param props a set of properties to be merged into the
     *        properties to be read and used for configuration.
     * @return a set of properties to be used as configuration.
     */
    public static Properties getProperties(Path[] additionalPaths) {
        try {
            // create a new object and load the default resources
            List<EzConfigurationLoader> configurationLoaders = new ArrayList<>();
            configurationLoaders.add(new DirectoryConfigurationLoader());
            configurationLoaders.add(new OpenShiftConfigurationLoader());

            for(Path p : additionalPaths) {
                configurationLoaders.add(new DirectoryConfigurationLoader(p));
            }

            EzConfigurationLoader [] loaders = new EzConfigurationLoader[configurationLoaders.size()];
            EzConfiguration ezConfiguration = new EzConfiguration(configurationLoaders.toArray(loaders));
            return ezConfiguration.getProperties();
        } catch(EzConfigurationLoaderException e) {
            throw new RuntimeException(e);
        }
    }
}

