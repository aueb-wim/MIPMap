package mipmapreduced;

import it.unibas.spicy.persistence.DAOException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.io.FilenameUtils;
import utils.ReadFiles;

public class MIPMapReduced {
  
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try{     
            if (args[0].equals("-unpivot")){
                unPivotCommand(args);
            } else if (args[0].equals("-csv_delimeter")){
                csvDelimeterCommand(args);
            }
            else {
                if (args.length > 4) {
                    printWrongInputMessage("translate");
                }                
                else {                 
                    String absoluteMappingTaskFilepath = FilenameUtils.separatorsToSystem(args[0]);
                    String exportPath = FilenameUtils.separatorsToSystem(args[1]);
                    String dbConfFile = FilenameUtils.separatorsToSystem(args[2]);
                    String exportDatabaseConfig = FilenameUtils.separatorsToSystem(args[3]);
                    boolean pkConstraints = true;
                    if (args.length == 5){                
                        String pk = args[4];
                        if (pk.equalsIgnoreCase("t") || pk.equalsIgnoreCase("true"))
                            pkConstraints = true;
                        else if (pk.equalsIgnoreCase("f") || pk.equalsIgnoreCase("false"))
                            pkConstraints = false;
                        else 
                            System.out.println("Last parameter must be either \"t\" (true) or \"f\" (false). Default value is true.");
                    }                

                    DirectoryChecker checker = new DirectoryChecker();
                    if (checker.checkFileValidity(absoluteMappingTaskFilepath) 
                        && checker.checkDirectoryValidity(exportPath)
                        && checker.checkFileValidity(dbConfFile)) {            
                        DbConnector dbconnect = new DbConnector();        
                        dbconnect.configureDatabaseProperties(dbConfFile); 

                        TaskHandler handleMappingTask = new TaskHandler(absoluteMappingTaskFilepath, exportPath, exportDatabaseConfig, pkConstraints);
                        handleMappingTask.performAction(); 
                    }
                    else {
                        System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
                        System.exit(-1);
                    }
                }
            }
        }        
        catch (Exception ex){
            System.err.println(ex);
            System.exit(-1);
        }        
    }
    
    private static void unPivotCommand(String[] args){
        if (args.length != 6) {
            printWrongInputMessage("unpivot");
        } else {
            try{
                String inputPath = FilenameUtils.separatorsToSystem(args[1]);
                String dbConfFile = FilenameUtils.separatorsToSystem(args[2]);
                String newColName = args[3];
                String commandForColumns = args[4];
                ReadFiles f = new ReadFiles(args[5]);
                String[] columns = f.readByLine();
                DirectoryChecker checker = new DirectoryChecker();
                if (checker.checkFileValidity(inputPath) && checker.checkFileValidity(dbConfFile)) { 
                    DbConnector dbconnect = new DbConnector();        
                    dbconnect.configureDatabaseProperties(dbConfFile); 
                    TaskHandler handleMappingTask = 
                            new TaskHandler(inputPath, commandForColumns, columns, newColName);
                    handleMappingTask.unPivot();
                } else {
                    System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
                }
            } catch (DAOException | IOException | SQLException ex){
                System.err.println(ex);
                System.exit(-1);
            }
        }
    }
    
    private static void csvDelimeterCommand(String[] args){
        if (args.length != 4) {
            printWrongInputMessage("csv_delimeter");
        } else {
            String inputPath = FilenameUtils.separatorsToSystem(args[1]);
            String sourceDelimiter = args[2];
            String sourceQuotes = args[3];
            if (sourceDelimiter.equalsIgnoreCase(";")) {
                sourceDelimiter = "Semi-colon";
            } else if (sourceDelimiter.equalsIgnoreCase(":")) {
                sourceDelimiter = "Colon";
            } else if (sourceDelimiter.equalsIgnoreCase("tab")) {
                sourceDelimiter = "Tab";
            } else {
                System.err.println("Wrong delimeter input");
                System.exit(-1);
            }
            
            if (sourceQuotes.equalsIgnoreCase("double")) {
                sourceQuotes = "Double quotes";
            } else if (sourceQuotes.equalsIgnoreCase("single")){
                sourceQuotes = "'";
            } else {
                System.err.println("Wrong quote input");
                System.exit(-1);
            }
            
            DirectoryChecker checker = new DirectoryChecker();
            if (checker.checkFileValidity(inputPath)) {            
                TaskHandler handleMappingTask = new TaskHandler(inputPath, sourceDelimiter, sourceQuotes);
                handleMappingTask.changeDelimeter();
            }
        }
    
    }
    
    private static void printWrongInputMessage(String option) {    
        if(option.equals("unpivot")){
            System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
            System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar "
                    + "-unpivot "
                    + "<Path To Mapping Csv File/SomeFile.csv> "
                    + "<new column name> " 
                    + "unpivoting command(-i(gnore) or -u(npivot)) "
                    + "<Path to column file/filename (columns(line by line) that included or excluded(regarding previous command selection) from unpivoting)>");
        } else if (option.equals("translate")){
            System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
            System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar "
                    + "<Path To Export Translated Instances> "
                    + "<Path To Database Configuration File/Database Configuration File> "
                    + "(true/false export possible Primary Key violations - optional)");
        } else if (option.equals("csv_delimeter")){
            System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
            System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar "
                    + "<Path To csv file/csvfile.csv> "
                    + "input parameter about current delimeter, possible choices: ;, :, tab "
                    + "input parameter about quotes, possible choices: single (for single quote), double (for double quote)");
        }
        System.exit(-1);
    }
    
}
