package utils;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/10 11:09
 * @Description 读写文件的操作。
 * <p>
 * ref: https://www.jb51.net/article/16396.htm
 * <p>
 * 注意：
 */
public class FileTools {

    private static File file;
    private static InputStream in;

    /**
     * 以字节为单位读取文件，常用于读二进制文件，如图片、声音、影像等文件。
     *
     * @param fileName 文件名
     */
    public static void readFileByBytes(String fileName) {

        file = new File(fileName);

        // 以字节为单位读取文件内容，一次读一个字节。
        try {
            in = new FileInputStream(file);
            int ch;
            while ((ch = in.read()) != -1) {
                System.out.println(ch);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 以字节为单位读取文件内容，一次读多个字节


        byte[] buffer = new byte[100];
        int ch;
        try {
            in = new FileInputStream(fileName);

            while ((ch = in.read(buffer)) != -1) {
                System.out.write(buffer, 0, ch);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 以字符为单位读取文件，常用于读文本，数字等类型的文件。
     *
     * @param fileName 文件名
     */
    public static void readFileByChars(String fileName) {

        file = new File(fileName);

        Reader reader = null;

        try {
            reader = new InputStreamReader(new FileInputStream(file));
            int ch;
            while ((ch = reader.read()) != -1) {
                if (((char) ch) != 'r') {
                    System.out.println((char) ch);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        char[] buffer = new char[30];
        int ch;

        try {
            reader = new InputStreamReader(new FileInputStream(fileName));

            while ((ch = reader.read(buffer)) != -1) {
                if ((ch == buffer.length) && (buffer[buffer.length - 1] != 'r')) {
                    System.out.println(buffer);
                } else {
                    for (int i = 0; i < ch; i++) {
                        if (buffer[i] != 'r') {
                            System.out.println(buffer[i]);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件.
     *
     * @param fileName 文件名
     */
    public static void readFileByLines(String fileName) {
        file = new File(fileName);

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));

            String buffer = null;

            while ((buffer = reader.readLine()) != null) {
                System.out.println(buffer);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 随机读取文件内容。
     *
     * @param fileName 文件名
     */
    public static void readFileByRandomAccess(String fileName) {

        file = new File(fileName);
        RandomAccessFile randomFile = null;

        try {
            randomFile = new RandomAccessFile(file, "r");

            long fileLength = randomFile.length();
            int beginIndex = (fileLength > 4) ? 4 : 0;
            randomFile.seek(beginIndex);
            byte[] bytes = new byte[10];
            int length;

            while ((length = randomFile.read(bytes)) != -1){
                System.out.write(bytes, 0 , length);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 方法（一）追加文件：使用 RandomAccessFile。
     *
     * @param fileName 文件名
     * @param content 追加的内容
     */
    public static void appendMethod1(String fileName, String content) {

        file = new File(fileName);

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            long fileLength = randomAccessFile.length();
            randomAccessFile.seek(fileLength);
            randomAccessFile.writeBytes(content);
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 方法（二）追加文件：使用 FileWriter。
     *
     * @param fileName 文件名
     * @param content 追加的内容
     */
    public static void appendMethod2(String fileName, String content) {

        file = new File(fileName);
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            fileWriter.write(content);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

