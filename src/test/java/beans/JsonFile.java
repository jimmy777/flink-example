package beans;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/10 9:14
 * @Description
 */
public class JsonFile {

    public static String readJsonFile(String fileName) {
        String jsonStr;

        File jsonFile = new File(fileName);
        FileReader fileReader = null;
        try {
            fileReader = new FileReader(jsonFile);


            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), "utf-8");

            int ch = 0;

            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char)ch);
            }

            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
