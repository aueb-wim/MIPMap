/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author ioannisxar
 */
public class ReadFiles {
    
    String fileName;
    
    public ReadFiles(String fileName){
        this.fileName = fileName;    
    }
    
    public String[] readByLine() throws IOException{
        ArrayList<String> columnList = new ArrayList();
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        try{
            String line;
            while((line = in.readLine()) != null) {
                columnList.add(line);
            }
        } catch(IOException ex){
            System.err.print(ex);
            System.exit(-1);
        } finally {
            in.close();
        }
        return columnList.toArray(new String[0]);
    }
}
