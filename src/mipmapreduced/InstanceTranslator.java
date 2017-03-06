package mipmapreduced;

import it.unibas.spicy.model.algebra.query.operators.sql.ExecuteSQL;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.persistence.relational.DAORelational;
import it.unibas.spicy.persistence.relational.DBFragmentDescription;
import it.unibas.spicy.persistence.relational.IConnectionFactory;
import it.unibas.spicy.persistence.relational.SimpleDbConnectionFactory;
import it.unibas.spicy.persistence.sql.DAOSql;
import it.unibas.spicy.utility.SpicyEngineConstants;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;


public class InstanceTranslator {
    public InstanceTranslator() {}
    
    public void performAction (MappingTask mappingTask, boolean pkConstraints, String exportPath) throws DAOException, SQLException, IOException{
        loadSourceInstancesBeforeTranslation(mappingTask.getSourceProxy(), 1);
        translateToDatabase(mappingTask, 1, pkConstraints, exportPath);  
    }
    
    private void translateToDatabase(MappingTask mappingTask, int scenarioNo, boolean pkConstraints, String exportPath) throws DAOException, IOException {
        ExecuteSQL exSQL = new ExecuteSQL();
        String sqltext = mappingTask.getMappingData().getSQLScript(scenarioNo);            
        exSQL.executeScript(mappingTask, sqltext, null, null, null, null,scenarioNo);        
        //in case there were any instances that violated the Primary Key constraints, ask the user to export those instances
        if (pkConstraints)
            checkForPKConstraints(mappingTask, exSQL.getPKConstraintsTables(), scenarioNo, exportPath);        
    }
    
    private void loadSourceInstancesBeforeTranslation(IDataSourceProxy dataSource, int scenarioNo) throws DAOException, SQLException{
        String datasourceType = dataSource.getType();
        if(datasourceType.equalsIgnoreCase("csv")){
            DAOCsv daoCsv = new DAOCsv();
            daoCsv.loadInstance(scenarioNo, dataSource, true);
        }
        else if(datasourceType.equalsIgnoreCase("sql")){
            DAOSql daoSql = new DAOSql();
            daoSql.loadInstance(scenarioNo, dataSource, true);
        }
        else if(datasourceType.equalsIgnoreCase("relational")){
            DAORelational daoRelational = new DAORelational();
            DBFragmentDescription dataDescription = new DBFragmentDescription();
            IConnectionFactory dataSourceDB = new SimpleDbConnectionFactory();
            AccessConfiguration accessConfiguration = (AccessConfiguration) dataSource.getAnnotation(SpicyEngineConstants.ACCESS_CONFIGURATION);
            daoRelational.loadInstance(scenarioNo, accessConfiguration, dataSource, dataDescription, dataSourceDB, true);
        }
    }
    
    private void checkForPKConstraints(MappingTask mappingTask, HashSet<String> pkTableNames, int scenarioNo, String exportPath) throws DAOException, IOException{
        if (!pkTableNames.isEmpty()){            
            DAOCsv daoCsv = new DAOCsv();
            try {
                daoCsv.exportPKConstraintCSVinstances(mappingTask, pkTableNames, exportPath, scenarioNo);                
                System.out.println();
                System.out.println("Translated instances which violate PK constraints successfully exported to " + exportPath); 
            }
            catch (DAOException ex) {
                System.out.println(ex);
            }           
        }
    }
}
