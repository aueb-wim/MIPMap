package mipmapreduced;

import it.unibas.spicy.utility.SpicyEngineConstants;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class DbConnector {
    //public static final String POSTGRESDB_CONFIGURATION_FILE = "postgresdb.properties";
    
    public DbConnector() {}
     
    //reads the properties file with database configuration and sets the appropriate variables 
    public void configureDatabaseProperties(String dbConfPath) {        
        Properties dbproperties = new Properties();            
        //InputStream stream = MIPMapReduced.class.getResourceAsStream("/conf/"+POSTGRESDB_CONFIGURATION_FILE);
        InputStream stream = null;
        try {
            stream = new FileInputStream(dbConfPath);                                                                                            
            dbproperties.load(stream);
            SpicyEngineConstants.setDatabaseParameters(dbproperties.getProperty("driver"),dbproperties.getProperty("uri"),
            dbproperties.getProperty("username"), dbproperties.getProperty("password"),dbproperties.getProperty("mappingTask-DatabaseName"));
        } catch (Exception ex) {
            System.out.println("Unable to load database configuration file");
        } finally {
            try {
                stream.close();
            }  catch (IOException ex) {
                System.out.println("Unable to load database configuration file");
            }
        }  
    }
}
