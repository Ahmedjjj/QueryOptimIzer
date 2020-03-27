package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    private int batchsize;                  // Number of tuples per out batch
    private ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    private String rfname;                  // The file name where the right table is materialized
    private Batch outbatch;                 // Buffer page for output
    private List<Batch> leftbatch;                // Blocks of buffer pages for left input stream
    private Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    private int nb_output_buffersl;
    private int lbuffcurs;                    // Cursor for left side block
    private int lcurs;                      // Cursor for left side buffer
    private int rcurs;                      // Cursor for right side buffer
    private boolean eosl;                   // Whether end of stream (left table) is reached
    private boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        nb_output_buffersl = numBuff - 2; //need to save one buffer for the right table and one for output
    }

    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;
        /** Right hand side table is to be materialized
         ** for the Block Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        return left.open() ? true : false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j, r;
        if (eosl && lbuffcurs == 0 && lcurs == 0 && eosr) {
            close();
            return null;
        }
        outbatch = new Batch((batchsize));
        if (lbuffcurs == 0 && lcurs == 0 && eosr) {
            /** new left block is to be fetched **/
            leftbatch.clear();
            while (leftbatch.size() < nb_output_buffersl) {
                Batch newBatch = (Batch) left.next();
                if (newBatch == null || newBatch.isEmpty()) {
                    eosl = true;
                    break;
                }
                leftbatch.add(newBatch);
            }

            if (leftbatch == null || leftbatch.isEmpty()) { //if the whole block is empty or null
                eosl = true;
                return outbatch;
            }

            /** Whenever a new left page came, we have to start the
             ** scanning of right table
             **/
            try {
                in = new ObjectInputStream(new FileInputStream(rfname));
                eosr = false;
                rcurs = 0;
            } catch (IOException io) {
                System.err.println("NestedJoin:error in reading the file");
                System.exit(1);
            }
        }
        while (eosr == false) {
            try {
                if (rcurs == 0 && lcurs == 0 && lbuffcurs == 0) {
                    rightbatch = (Batch) in.readObject();
                    if (rightbatch == null || rightbatch.isEmpty()) {
                        throw new EOFException("No more tuples in right table");
                    }
                }
                for (i = lbuffcurs; i < leftbatch.size(); i++) {
                    Batch lbatch = leftbatch.get(i);
                    for (j = lcurs; j < lbatch.size(); j++) {
                        Tuple lefttuple = lbatch.get(j);
                        for (r = rcurs; r < rightbatch.size(); r++) {
                            Tuple righttuple = rightbatch.get(r);
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                outbatch.add(lefttuple.joinWith(righttuple));
                                if (outbatch.isFull()) {
                                    if (r != rightbatch.size() - 1) {//case 1: current right batch has tuples
                                        lbuffcurs = i;
                                        lcurs = j;
                                        rcurs = r + 1;
                                    } else if (j != lbatch.size() - 1) { //case 2: current left batch hasn't completed
                                        lbuffcurs = i;
                                        lcurs = j + 1;
                                        rcurs = 0;
                                    } else if (i != leftbatch.size() - 1) {//case 3: left block still has batches
                                        lbuffcurs = i + 1;
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else {// case 4: everything has been compared
                                        lbuffcurs = 0;
                                        lcurs = 0;
                                        rcurs = 0;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                }
                lbuffcurs = 0;
            } catch (EOFException e) {
                try {
                    in.close();
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin:Error in temporary file reading");
                }
                eosr = true;
            } catch (ClassNotFoundException c) {
                System.out.println("BlockNestedJoin:Some error in deserialization ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("BlockNestedJoin:temporary file reading error");
                System.exit(1);
            }
        }

        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }


}

