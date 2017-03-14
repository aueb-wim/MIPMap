package mipmapreduced;

import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOHandleDB;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.persistence.csv.ChangeDelimiterCSV;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.persistence.csv.UnpivotCSVDAO;
import it.unibas.spicy.persistence.sql.DAOSql;
import it.unibas.spicy.utility.SpicyEngineConstants;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import utils.ReadFiles;

public class TaskHandler {
    private String fileAbsoluteFile, exportDatabaseConfig, targetPath, newColName, 
            commandForColumns, sourceDelimiter, sourceQuotes, exportCommand;
    private boolean pkConstraints;
    private String[] selectedColumns;
    public TaskHandler(String fileAbsoluteFile, String exportCommand, String targetPath, String exportDatabaseConfig, boolean pkConstraints) {
        this.fileAbsoluteFile = fileAbsoluteFile;
        this.exportCommand = exportCommand;
        this.targetPath = targetPath;
        this.pkConstraints = pkConstraints;
        this.exportDatabaseConfig = exportDatabaseConfig;
        
    }
    
    public TaskHandler(String fileAbsoluteFile, String commandForColumns, String[] selectedColumns, String newColName) {
        this.fileAbsoluteFile = fileAbsoluteFile;
        this.selectedColumns = selectedColumns;
        this.newColName = newColName;
        this.commandForColumns = commandForColumns;
    }
    
    public TaskHandler(String fileAbsoluteFile, String sourceDelimiter, String sourceQuotes) {
        this.fileAbsoluteFile = fileAbsoluteFile;
        this.sourceDelimiter = sourceDelimiter;
        this.sourceQuotes = sourceQuotes;
    }
    
    public void unPivot() throws DAOException, SQLException, IOException{
        ArrayList<String> colNames = new ArrayList();
        ArrayList<String> keepColNames = new ArrayList();
        try {
            UnpivotCSVDAO daoUnpivot = new UnpivotCSVDAO();
            File file = new File(fileAbsoluteFile);
            String[] columnNames = daoUnpivot.getCsvTableColumnsWithoutSpecialCharacters(file);
            if (commandForColumns.equals("-i")){
                for(String col: columnNames){
                    
                    if (Arrays.asList(selectedColumns).contains(col)){
                        keepColNames.add(col);
                    } else {
                        colNames.add(col);
                    }
                }
            } else if (commandForColumns.equals("-u")) {
                for(String col: columnNames){
                    
                    if (!Arrays.asList(selectedColumns).contains(col)){
                        keepColNames.add(col);
                    } else {
                        colNames.add(col);
                    }
                }
            } else {
                System.err.println("Wrong unpivot command");
                System.exit(-1);
            }          
            daoUnpivot.unpivotTable(keepColNames, colNames, newColName, file);
        } catch (IOException ex) {
            System.out.println(ex);
            System.exit(-1);
        } 
    }
    
    public void changeDelimeter() {
        
        try{
            File file = new File(fileAbsoluteFile);
            ChangeDelimiterCSV changer = new ChangeDelimiterCSV();
            changer.changeDelimiter(file, sourceDelimiter, sourceQuotes, true);
        } catch (IOException ex){
            System.err.println(ex);
            System.exit(-1);
        }
    }
    
    
    public void performAction() {
        createDB();
        DAOMappingTask daoMappingTask = new DAOMappingTask();
        MappingTask mappingTask;
        try {
            mappingTask = daoMappingTask.loadMappingTask(1, fileAbsoluteFile, SpicyEngineConstants.LINES_BASED_MAPPING_TASK, false);
            
            InstanceTranslator translate = new InstanceTranslator();
            translate.performAction(mappingTask, pkConstraints , targetPath);
            
            if(exportCommand.equals("-csv")){
                ReadFiles folder = new ReadFiles(targetPath);
                DAOCsv daoCsv = new DAOCsv();
                if(folder.openTargetDirectory()==0) {    
                    daoCsv.exportTranslatedCSVinstances(mappingTask, targetPath, 1);
                } else {
                    daoCsv.appendTranslatedCSVinstances(mappingTask, folder.getPaths(), 1);
                }
            } else if(exportCommand.equals("-db")) {
                ReadFiles f = new ReadFiles(exportDatabaseConfig);
                ArrayList<String> config = f.getExportDatabaseConfig();
                DAOSql dao = new DAOSql();
                dao.exportTranslatedSQLInstances(mappingTask, 1, config.get(0), 
                        config.get(1)+config.get(4), config.get(2), config.get(3));
            }
            System.out.println();
            System.out.println("Translate instances successfully!");            
        } catch (DAOException | SQLException | IOException ex) {
            System.out.println(ex);
            System.exit(-1);
        }            
    }
    
    private void createDB(){
        DAOHandleDB daoCreateDB = new DAOHandleDB();
        try {
            daoCreateDB.createNewDatabase();
        } catch (DAOException ex) {
            System.out.println(ex);
            System.exit(-1);
        }
    }
    
}
