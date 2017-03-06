package mipmapreduced;

import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.utility.SpicyEngineConstants;
import java.io.IOException;
import java.sql.SQLException;

public class TaskHandler {
    private String fileAbsoluteFile;
    private String targetPath;
    private boolean pkConstraints;
        
    public TaskHandler(String fileAbsoluteFile, String targetPath, boolean pkConstraints) {
        this.fileAbsoluteFile = fileAbsoluteFile;
        this.targetPath = targetPath;
        this.pkConstraints = pkConstraints;
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
