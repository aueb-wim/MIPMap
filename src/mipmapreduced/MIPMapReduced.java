package mipmapreduced;

import static org.apache.commons.io.FilenameUtils.separatorsToSystem;

public class MIPMapReduced {

    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try{        
            if (args.length > 4) {
                printWrongInputMessage();
            }                
            else {                 
                String absoluteMappingTaskFilepath = separatorsToSystem(args[0]);
                String exportPath = separatorsToSystem(args[1]);
                String dbConfFile = separatorsToSystem(args[2]);
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
        catch (ArrayIndexOutOfBoundsException ex){
            printWrongInputMessage();
        }        
        catch (Exception ex){
            System.out.println(ex);
            ex.printStackTrace();
        }        
    }
    
    private static void printWrongInputMessage() {        
        System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
        System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar <Path To Mapping Task File/Mapping Task File.xml> "
                + "<Path To Export Translated Instances> "
                + "<Path To Database Configuration File/Database Configuration File> "
                + "(true/false export possible Primary Key violations - optional)");
    }
    
}
