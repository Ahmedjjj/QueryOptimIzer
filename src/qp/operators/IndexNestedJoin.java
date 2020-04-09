package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class IndexNestedJoin extends Join{

    static int filenum = 0;         // To get unique filenum for this operation
    private int batchsize;                  // Number of tuples per out batch
    private int leftindex;   // Indices of the join attributes in left table
    private int rightindex;  // Indices of the join attributes in right table
    private String rfname;                  // The file name where the right table is materialized
    private Batch leftbatch;                // Blocks of buffer pages for left input stream
    private Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    private int lbuffcurs;                    // Cursor for left side block
    private int lcurs;                      // Cursor for left side buffer
    private int rcurs;                      // Cursor for right side buffer
    private boolean eosl;                   // Whether end of stream (left table) is reached
    private boolean eosr;                   // Whether end of stream (right table) is reached

    IndexBuilder indexTable;                // Build index for right table
    ArrayList<Tuple> matches;// Number for matching tuples

    Join join;

    public IndexNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        this.join = jn;
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open(){

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize()/tuplesize;

        /** find indices attributes of join conditions **/
        Attribute leftattr = conditionList.get(0).getLhs();
        Attribute rightattr = (Attribute) conditionList.get(0).getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        /** initialize the cursors of input buffers **/
        eosl = false;
        lcurs = 0;

        Batch rightBatch;
        /** Load table into index **/
        if(!right.open()){
            return false;
        } else {
            rcurs = 0;
            indexTable = new IndexBuilder();
            indexTable.setIndex(rightindex);
            while((rightBatch = right.next()) != null){
                indexTable.createIndex(rightBatch);
            }
            matches = new ArrayList<Tuple>();
        }

        return left.open() ? true : false;
    }

    public Batch next(){
        /**Close when processing is being completed **/
        if(lcurs == 0 && eosl == true){
            close();
            return null;
        } else{
            return generateOutbatch();
        }
    }

    public Batch generateOutbatch() {
        /** Buffer page for output **/
        Batch outbatch = new Batch(batchsize);
        /** Adds tuples to outbatch **/
        while(outbatch.isFull() == false){
            /** Gets next leftbatch **/
            if(lcurs == 0){
                leftbatch = left.next();
                if(leftbatch == null) {
                    eosl = true;
                    return outbatch;
                }
            }
            /** Check remaining tuples in right table **/
            if (rcurs != 0) {
                Tuple latestlefttuple = leftbatch.get(lcurs);
                for (int i = rcurs; i < matches.size(); i++) {
                    Tuple righttuple = matches.get(i);
                    if (latestlefttuple.checkJoin(righttuple, leftindex, rightindex) == true) {
                    Tuple outbatchtuple = latestlefttuple.joinWith(matches.get(i));
                    outbatch.add(outbatchtuple);
                        if(outbatch.isFull() == true){
                        /** Determine next position of lcurs and rcurs  **/
                            int matchCount = matches.size() - 1;
                                if(i == matchCount){
                                    lcurs = lcurs + 1;
                                    lcurs = lcurs % leftbatch.size();
                                    rcurs = 0;
                                } else {
                                    rcurs = i + 1;
                                }
                            return outbatch;
                        }
                    }
                }
            lcurs = (lcurs + 1);
            lcurs = lcurs % leftbatch.size();
            rcurs = 0;
        }
        /** Join matching tuples  **/    
        for (int i = lcurs; i < leftbatch.size(); i++) {
            Tuple lefttuple = leftbatch.get(i);
            matches = indexTable.get(lefttuple.dataAt(leftindex));
            if (matches != null) {
                if (matches.isEmpty() == false){
                    for (int j = rcurs; j < matches.size(); j++) {
                        Tuple righttuple = matches.get(j);
                        if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                            Tuple outbatchtuple = lefttuple.joinWith(matches.get(j));
                            outbatch.add(outbatchtuple);
                            if (outbatch.isFull() == true) {
                                int matchCount = matches.size() - 1;
                                if (j == matchCount) {
                                    lcurs = (i + 1);
                                    lcurs = lcurs % leftbatch.size();
                                    rcurs = 0;
                                } else {
                                    lcurs = i;
                                    rcurs = j + 1;
                                }
                                return outbatch;
                            }
                        }
                    }
                }
            }
            rcurs = 0;
        }
        lcurs = 0;
    }
        return outbatch;
    }
    public Object clone() {
        Join join = (Join) this.join.clone();
        IndexNestedJoin newJoin = new IndexNestedJoin(join);
        newJoin.setSchema((Schema)this.schema.clone());
        return newJoin;
    }
    /**
     * Close the operator
     **/
    public boolean close() {
        return right.close() && left.close();
    }

}
