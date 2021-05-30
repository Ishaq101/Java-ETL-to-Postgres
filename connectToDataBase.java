package advancedjava_hw2_w12;
import java.sql.*;
import java.io.*;
import java.util.Scanner;


public class connectToDataBase {
    public static void main(String[] args) {
//        Scanner sc = null;
//        sc.useDelimiter(",");
        int batchSize = 20;

        String userName = "digitalskola";
        String password = "digitalskola";
        // "jdbc:postgresql://host:port/database?user=username&password=password"
        String connectionString = "jdbc:postgresql://127.0.0.1:5432/etl_db?user="+userName+"&password="+password;

        String createTablequery = "CREATE TABLE TINGKAT_PENDIDIKAN "+
                "(kode_provinsi INTEGER not NULL, "+
                "nama_provinsi VARCHAR(255), "+
                "tingkat_pendidikan VARCHAR(255), "+
                "jenis_kelamin VARCHAR(20), "+
                "jumlah_individu INTEGER not NULL)";

        String insertQuery = "INSERT INTO TINGKAT_PENDIDIKAN (kode_provinsi, nama_provinsi, tingkat_pendidikan, jenis_kelamin, jumlah_individu) VALUES (?, ?, ?, ?, ?)";
        String selectQuery = "SELECT * FROM TINGKAT_PENDIDIKAN";



        try {
            String csvFilePath = "/home/ishaq/Documents/belajar_java_1/advanced_java_hw/advancedjava_hw2_w12/data.csv";
            Scanner sc = new Scanner(new File(csvFilePath));
            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;
            lineReader.readLine(); //skip header line
            int count = 0;


            Connection conn = DriverManager.getConnection(connectionString);
            Statement stmnt = conn.createStatement();
            PreparedStatement prepstmnt = conn.prepareStatement(insertQuery);
            conn.setAutoCommit(false);

            while ((lineText = lineReader.readLine()) != null){
                String[] data = lineText.split(",");
                int kode_provinsi = Integer.parseInt(data[0]);
                String nama_provinsi = data[1];
                String tingkat_pendidikan = data[2];
                String jenis_kelamin = data[3];
                int jumlah_individu = Integer.parseInt(data[4]);

                prepstmnt.setInt(1, kode_provinsi);
                prepstmnt.setString(2,nama_provinsi);
                prepstmnt.setString(3,tingkat_pendidikan);
                prepstmnt.setString(4,jenis_kelamin);
                prepstmnt.setInt(5, jumlah_individu);

                if (count%batchSize == 0){
                    prepstmnt.executeBatch();
                }


            }
            lineReader.close();

            //execute the remaining queries
            //prepstmnt.executeBatch();
            //stmnt.execute(selectQuery);
            conn.commit();
            // MAKE TABLE
//            stmnt.executeUpdate(createTablequery);
//            stmnt.executeUpdate(selectQuery);
            prepstmnt.close();


            stmnt.close();
            conn.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        catch(SQLException e){
            e.printStackTrace();
        }
    }
}
