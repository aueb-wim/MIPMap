package mipmapreduced;

import java.io.File;


public class DirectoryChecker {
    private String invalidFilePath = null;
    
    public boolean checkDirectoryValidity(String directoryPath) {
        File directory = new File (directoryPath);
        if (directory.isDirectory()) {
            return true;
        }
        else {
            invalidFilePath = directoryPath;
            return false;
        }
    }
    
    public boolean checkFileValidity(String filePath) {
        File directory = new File (filePath);
        if (directory.exists()) {
            return true;
        }
        else {
            invalidFilePath = filePath;
            return false;
        }
    }
    
    public String getInvalidFilePath(){
        return invalidFilePath;
    }
}
