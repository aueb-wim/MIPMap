package mipmapreduced;

import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.persistence.csv.UnpivotCSVDAO;
import it.unibas.spicy.utility.SpicyEngineConstants;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class TaskHandler {
    private String fileAbsoluteFile;
    private String targetPath, newColName;
    private boolean pkConstraints;
    private String[] selectedColumns;
    public TaskHandler(String fileAbsoluteFile, String targetPath, boolean pkConstraints) {
        this.fileAbsoluteFile = fileAbsoluteFile;
        this.targetPath = targetPath;
        this.pkConstraints = pkConstraints;
        
    }
    
    public TaskHandler(String fileAbsoluteFile, String[] selectedColumns, String newColName) {
        this.fileAbsoluteFile = fileAbsoluteFile;
        this.selectedColumns = selectedColumns;
        this.newColName = newColName;
    }
    
    public void unPivot() throws DAOException, SQLException, IOException{
        ArrayList<String> colNames = new ArrayList();
        ArrayList<String> keepColNames = new ArrayList();
        try {
            UnpivotCSVDAO daoUnpivot = new UnpivotCSVDAO();
            File file = new File(fileAbsoluteFile);
            String[] columnNames = daoUnpivot.getCsvTableColumns(file);
            //we keep all columns for pivoting
            if (selectedColumns.length == 1 && selectedColumns[0].equalsIgnoreCase("keepall")){
                for(String col: columnNames){
                    colNames.add(col);
                }
            } else {
                for(String col: columnNames){
                    if (!Arrays.asList(selectedColumns).contains(col)){
                        keepColNames.add(col);
                    } else {
                        colNames.add(col);
                    }
                }
            }
            for(String item: colNames){
                System.out.println(item);
            }
            System.out.println("---");
            for(String item: keepColNames){
                System.out.println(item);
            }
            daoUnpivot.unpivotTable(keepColNames, colNames, newColName, file);
        } catch (IOException ex) {
            System.out.println(ex);
            ex.printStackTrace();
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
            ex.printStackTrace();
        }            
    }
    
}
