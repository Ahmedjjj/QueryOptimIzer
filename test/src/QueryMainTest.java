import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class QueryMainTest {

    public static void main(String[] args) {
        boolean test1 = test("query1.sql", "query1.out", "customer.txt",4096, 50);
        System.out.println("Test 1 --- " + (test1 ? "Passed" : "Failed"));
        
    }
    private static boolean test(String queryFileName, String resultFileName, String expectedResultFileName, int pageSize, int numBuffer) {
        String[] args = {queryFileName, resultFileName, Integer.toString(pageSize), Integer.toString(numBuffer)};
        QueryMain.main(args);
        Path resultPath = Paths.get("." , resultFileName);
        Path expectedResultPath = Paths.get("." , expectedResultFileName);
        try {
            byte[] f1 = Files.readAllBytes(resultPath);
            byte[] f2 = Files.readAllBytes(expectedResultPath);
            return Arrays.equals(f1,f2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
