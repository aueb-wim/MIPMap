package mipmapreduced;

import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.persistence.csv.ChangeDelimiterCSV;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.persistence.csv.UnpivotCSVDAO;
import it.unibas.spicy.utility.SpicyEngineConstants;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class TaskHandler {
    private String fileAbsoluteFile, targetPath, newColName, commandForColumns, sourceDelimiter, sourceQuotes;
    private boolean pkConstraints;
    private String[] selectedColumns;
    public TaskHandler(String fileAbsoluteFile, String targetPath, boolean pkConstraints) {
        this.fileAbsoluteFile = fileAbsoluteFile;
        this.targetPath = targetPath;
        this.pkConstraints = pkConstraints;
        
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
            String[] columnNames = daoUnpivot.getCsvTableColumns(file);
            
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
        DAOMappingTask daoMappingTask = new DAOMappingTask();
        MappingTask mappingTask;
        try {
            mappingTask = daoMappingTask.loadMappingTask(1, fileAbsoluteFile, SpicyEngineConstants.LINES_BASED_MAPPING_TASK, false);
            
            InstanceTranslator translate = new InstanceTranslator();
            translate.performAction(mappingTask, pkConstraints , targetPath);
            
            DAOCsv daoCsv = new DAOCsv();   
            daoCsv.exportTranslatedCSVinstances(mappingTask, targetPath, 1);
            
            System.out.println();
            System.out.println("Translated instances successfully exported to "+targetPath);            
        } catch (DAOException | SQLException | IOException ex) {
            System.out.println(ex);
            System.exit(-1);
        }            
    }
    
}
