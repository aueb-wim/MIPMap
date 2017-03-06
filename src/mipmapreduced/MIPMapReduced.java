package mipmapreduced;

import org.apache.commons.io.FilenameUtils;

public class MIPMapReduced {

    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try{     
            if (args[0].equals("-unpivot")){
                if (args.length != 5) {
                    printWrongInputMessage("unpivot");
                }else {
                    String inputPath = FilenameUtils.separatorsToSystem(args[1]);
                    String dbConfFile = FilenameUtils.separatorsToSystem(args[2]);
                    String newColName = args[3];
                    String[] columns = args[4].split(",");
                    DirectoryChecker checker = new DirectoryChecker();
                    if (checker.checkFileValidity(inputPath) && checker.checkFileValidity(dbConfFile))
                    { 
                        DbConnector dbconnect = new DbConnector();        
                        dbconnect.configureDatabaseProperties(dbConfFile); 
                        TaskHandler handleMappingTask = 
                                new TaskHandler(inputPath, columns, newColName);
                        handleMappingTask.unPivot();
                    } else {
                        System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
                    }
                }
            } else {
                if (args.length > 4) {
                    printWrongInputMessage("translate");
                }                
                else {                 
                    String absoluteMappingTaskFilepath = FilenameUtils.separatorsToSystem(args[0]);
                    String exportPath = FilenameUtils.separatorsToSystem(args[1]);
                    String dbConfFile = FilenameUtils.separatorsToSystem(args[2]);
                    boolean pkConstraints = true;
                    if (args.length == 4){                
                        String pk = args[3];
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
                        && checker.checkFileValidity(dbConfFile)) 
                    {            
                        DbConnector dbconnect = new DbConnector();        
                        dbconnect.configureDatabaseProperties(dbConfFile); 

                        TaskHandler handleMappingTask = new TaskHandler(absoluteMappingTaskFilepath, exportPath, pkConstraints);
                        handleMappingTask.performAction(); 
                    }
                    else {
                        System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
                    }
                }
            }
            
        }        
        catch (Exception ex){
            System.out.println(ex);
            ex.printStackTrace();
        }        
    }
    
    private static void printWrongInputMessage(String option) {    
        if(option.equals("unpivot")){
            System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
            System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar "
                    + "-unpivot "
                    + "<Path To Mapping Csv File/SomeFile.csv> "
                    + "<new column name> "
                    + "<columns that include in pivoting, comma separated or keepall to keep all columns> ");
        } else if (option.equals("translate")){
            System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
            System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar <Path To Mapping Task File/Mapping Task File.xml> "
                    + "<Path To Export Translated Instances> "
                    + "<Path To Database Configuration File/Database Configuration File> "
                    + "(true/false export possible Primary Key violations - optional)");
        }
        
    }
    
}
