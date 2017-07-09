/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 *
 * @author ioannisxar
 */
public class ReadFiles {
    
    String fileName;
    
    public ReadFiles(String fileName){
        this.fileName = fileName;    
    }
    
    //avenet
    public String readByLineSimple() throws IOException {
        String script = "";
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        try{
            String line;
            while((line = in.readLine()) != null) {
                String col = line.replace("\"", "");
                script=script+col+"\n";
            }
        } catch(IOException ex){
            System.err.print(ex);
            System.exit(-1);
        } finally {
            in.close();
        }
        return script;
    }
    
    public String[] readByLine() throws IOException {
        ArrayList<String> columnList = new ArrayList();
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        try{
            String line;
            while((line = in.readLine()) != null) {
                String col = line.replace("\"", "");
                Pattern p = Pattern.compile("[^a-zA-Z0-9]");
                if (p.matcher(col).find()) {
                    col = col.replaceAll("[\\W]|_", "_");
                }
                columnList.add(col);
            }
        } catch(IOException ex){
            System.err.print(ex);
            System.exit(-1);
        } finally {
            in.close();
        }
        return columnList.toArray(new String[0]);
    }
    
    public ArrayList<String> getExportDatabaseConfig() throws FileNotFoundException, IOException{
        ArrayList<String> config = new ArrayList<>();
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        try{
            String line;
            while((line = in.readLine()) != null) {
                config.add(line.split("=")[1].trim());
            }
        } catch(IOException ex){
            System.err.print(ex);
            System.exit(-1);
        } finally {
            in.close();
        }
        return config;
    }
    
    public HashMap<String, String> getPaths(){
        File folder = new File(fileName); 
        HashMap<String, String> paths = new HashMap<>();
        for (final File fileEntry : folder.listFiles()) {
            if(fileEntry.getName().contains(".csv"))
                paths.put(fileEntry.getName().replace(".csv","").trim(), fileName+"/"+fileEntry.getName());
        }
        return paths;
    }
    
    public int openTargetDirectory() {
        int num = 0;
        File folder = new File(fileName); 
        for (final File fileEntry : folder.listFiles()) {
            if(fileEntry.getName().contains(".csv"))
                num++;
        }
        return num;
    }
    
}
