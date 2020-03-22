import qp.operators.*;
import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class SortTest {
    private static final int PAGE_SIZE = 128;
    private static final int NUM_BUFFERS = 3;

    public static void main(String[] args) {
        Batch.setPageSize(PAGE_SIZE);
        BufferManager.setNumBuffer(NUM_BUFFERS);
        Operator scan = new Scan(args[0], OpType.SCAN);
        Schema schema = null;
        try {
            ObjectInputStream schemaInputStream = new ObjectInputStream(new FileInputStream(args[0] + ".md"));
            schema = (Schema) schemaInputStream.readObject();
            scan.setSchema(schema);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        scan.open();

//        ArrayList<Attribute> attributes = new ArrayList<>(schema.getAttList());
//        attributes.remove(0);
//        attributes.remove(0);
//        Schema projectSchema = schema.subSchema(attributes);
//
//        Operator project = new Project(scan, attributes,OpType.PROJECT);
//        project.setSchema(projectSchema);
//        project.open();

        Operator sort = new Sort(schema.getAttList(),scan,false);
        int numAtts = schema.getNumCols();
        Batch batch;
        sort.open();
        int totalsize = 0;
        while ((batch = sort.next()) != null){
            batch.getTuples().forEach(t -> printTuple(t,numAtts));
            totalsize += batch.getTuples().size();

        }
       // System.out.println(totalsize);
        sort.close();
    }

    public static void printTuple(Tuple t,int numAtts) {
        for (int i = 0; i < numAtts; ++i) {
            Object data = t.dataAt(i);
            if (data == null) {
                System.out.print("-NULL-\t");
            } else {
                System.out.print(data + "\t");
            }
        }
        System.out.println();
    }
}
