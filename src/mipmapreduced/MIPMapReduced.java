package mipmapreduced;

import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.relational.IConnectionFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.io.FilenameUtils;
import utils.ReadFiles;
import it.unibas.spicy.persistence.relational.SimpleDbConnectionFactory;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class MIPMapReduced {
  
    /**
     * @param args the command line arguments
     * @author ioannisxar
     */
    public static void main(String[] args) {
        try{     
            if (args[0].equals("-unpivot")) {
                unPivotCommand(args);
            } else if (args[0].equals("-csv_delimeter")) {
                csvDelimeterCommand(args);
            } else if(args[0].equals("-generate_id")){
                generateId(args);
            } else if(args[0].equals("-runsql")){
                runsql(args);
            }
            else {
                translateCommand(args);
            }
        }        
        catch (Exception ex){
            System.err.println(ex);
            System.exit(-1);
        }        
    }
    
    //avenet
    private static void runsql(String[] args) throws IOException, DAOException, SQLException {
        DirectoryChecker checker = new DirectoryChecker();
        String dbConfFile = FilenameUtils.separatorsToSystem(args[2]);
        String sqlFile = FilenameUtils.separatorsToSystem(args[1]);
        if (checker.checkFileValidity(dbConfFile) && checker.checkFileValidity(sqlFile)) {
            ReadFiles f = new ReadFiles(dbConfFile);
            ArrayList<String> config = f.getExportDatabaseConfig();
            ReadFiles sqlReader = new ReadFiles(sqlFile);
            String sqlScript = sqlReader.readByLineSimple();
            IConnectionFactory connectionFactory = new SimpleDbConnectionFactory();
            try (Connection connection = getConnectionToDatabase(connectionFactory, config.get(0),config.get(1)+config.get(4), config.get(2), config.get(3))) {
                Statement statement = connection.createStatement();
                statement.executeUpdate(sqlScript);
            }
            System.out.println("SQL script executed successfully!");
        } else {
            System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
            System.exit(-1);
        }     
    }
    
    //avenet
    private static Connection getConnectionToDatabase(IConnectionFactory connectionFactory, String driver, String uri, String login, String pass) throws DAOException{
        AccessConfiguration accessConfiguration = new AccessConfiguration();
        accessConfiguration.setDriver(driver);
        accessConfiguration.setUri(uri);
        accessConfiguration.setLogin(login);
        accessConfiguration.setPassword(pass);
        return connectionFactory.getConnection(accessConfiguration);
    }

    private static void generateId(String[] args) throws IOException, SQLException, ClassNotFoundException {
        String generateIdFilePath = FilenameUtils.separatorsToSystem(args[1]);
        DirectoryChecker checker = new DirectoryChecker();
        if (checker.checkFileValidity(generateIdFilePath)) {
            TaskHandler handleMappingTask = new TaskHandler(generateIdFilePath);
            handleMappingTask.generateId();
        }
    }
    
    private static void translateCommand(String[] args){
        if (args.length > 5) {
            printWrongInputMessage("translate");
        }                
        else {                 
            String absoluteMappingTaskFilepath = FilenameUtils.separatorsToSystem(args[0]);
            String dbConfFile = FilenameUtils.separatorsToSystem(args[1]);
            String exportCommand = FilenameUtils.separatorsToSystem(args[2]);
            String exportDatabaseConfig = null;
            String exportPath = null;
            boolean pkConstraints = true;
            DirectoryChecker checker = new DirectoryChecker();
            if(exportCommand.equals("-db")){
                exportDatabaseConfig = FilenameUtils.separatorsToSystem(args[3]);
            } else if (exportCommand.equals("-csv")){
                exportPath = FilenameUtils.separatorsToSystem(args[3]);
                if (!checker.checkDirectoryValidity(exportPath)) {
                    System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
                    System.exit(-1);
                }
                if (args.length == 5){                
                    String pk = args[4];
                    if (pk.equalsIgnoreCase("t") || pk.equalsIgnoreCase("true"))
                        pkConstraints = true;
                    else if (pk.equalsIgnoreCase("f") || pk.equalsIgnoreCase("false"))
                        pkConstraints = false;
                    else 
                        System.out.println("Last parameter must be either \"t\" (true) or \"f\" (false). Default value is true.");
                }                
            } else {
                System.out.println("Wrong export command!");
                System.exit(-1);
            }
            if (checker.checkFileValidity(absoluteMappingTaskFilepath) 
                && checker.checkFileValidity(dbConfFile)) {            
                DbConnector dbconnect = new DbConnector();        
                dbconnect.configureDatabaseProperties(dbConfFile); 

                TaskHandler handleMappingTask = new TaskHandler(absoluteMappingTaskFilepath, exportCommand, exportPath, 
                        exportDatabaseConfig, pkConstraints);
                handleMappingTask.performAction(); 
            }
            else {
                System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
                System.exit(-1);
            }
        }  
    }
    
    private static void unPivotCommand(String[] args){
        if (args.length != 7) {
            printWrongInputMessage("unpivot");
        } else {
            try{
                String inputPath = FilenameUtils.separatorsToSystem(args[1]);
                String dbConfFile = FilenameUtils.separatorsToSystem(args[2]);
                String newColName = args[3];
                ReadFiles f1 = new ReadFiles(args[4]);
                String[] columns = f1.readByLine();
                String commandForColumns = args[5];
                ReadFiles f = new ReadFiles(args[6]);
                String[] columnsToAlter = f.readByLine();
                DirectoryChecker checker = new DirectoryChecker();
                if (checker.checkFileValidity(inputPath) && checker.checkFileValidity(dbConfFile)) { 
                    DbConnector dbconnect = new DbConnector();        
                    dbconnect.configureDatabaseProperties(dbConfFile); 
                    TaskHandler handleMappingTask = 
                            new TaskHandler(inputPath, columns, commandForColumns, columnsToAlter, newColName);
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
                    + "<Path To Database Configuration File/Database Configuration File> "
                    + "<new column name> " 
                    + "<Path To selected columns file/filename> "
                    + "unpivoting command(-i(gnore) or -u(npivot)) "
                    + "<Path to altered column file/filename (columns(line by line) that included or excluded(regarding previous command selection) from unpivoting)>");
        } else if (option.equals("translate")){
            System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
            System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar "
                    + "<Path To Export Translated Instances> "
                    + "<Path To Database Configuration File/Database Configuration File> "
                    + "<Path To Export Database Configuration File/Export Database Configuration File> OR -csv To Export On CSV file in the target path "
                    + "(true/false export possible Primary Key violations - optional)");
        } else if (option.equals("csv_delimeter")){
            System.out.println("\nWrong input. For correct MIPMapReduced usage run jar as:");
            System.out.println("java -jar <Path To Jar>/MIPMapReduced.jar "
                    + "-csv_delimeter "
                    + "<Path To csv file/csvfile.csv> "
                    + "input parameter about current delimeter, possible choices: \";\", \":\", tab "
                    + "input parameter about quotes, possible choices: single (for single quote), double (for double quote)");
        }
        System.exit(-1);
    }    
    
}
