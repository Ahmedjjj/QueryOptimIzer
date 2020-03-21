package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public final class Sort extends Operator {

    private final List<Attribute> sortAttributes;
    private final Operator base;
    private final List<Integer> attributeIndexes;
    private final Comparator<Tuple> comparator;
    private final int batchSize;

    private List<File> sortedRuns;
    private ObjectInputStream batchInputStream;

    public Sort (List<Attribute> sortAttributes, Operator base){
        super(OpType.SORT);
        this.base = base;
        this.sortAttributes = sortAttributes;
        this.comparator = getComparator();
        this.attributeIndexes = getAttributeIndexes();
        this.batchSize = Batch.getPageSize() / base.getSchema().getTupleSize();

    }

    @Override
    public boolean open() {
        File file = sort();
        try {
            batchInputStream = new ObjectInputStream(new FileInputStream(file.getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            batchInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public Batch next() {
        try {
            return (Batch) batchInputStream.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<Integer> getAttributeIndexes (){
        ArrayList<Attribute> schemaAttributes = base.getSchema().getAttList();
        return sortAttributes.stream().map((a) -> schemaAttributes.indexOf(a)).collect(Collectors.toList());
    }
    private Comparator<Tuple> getComparator (){
        return (t1 , t2) -> Tuple.compareTuples(t1, t2, attributeIndexes, attributeIndexes);
    }

    private File sort (){
        int numRuns = 0;
        while (generateRun("run" + numRuns )){
            numRuns ++;
        }
        File mergedFile = mergeRuns(sortedRuns);
        return mergedFile;
    }
    private File mergeRuns (List<File> runs){
        if (runs.isEmpty()){
            throw new IllegalArgumentException();
        }
        if (runs.size() == 1){
            return runs.get(0);
        }

        int numBuffers = BufferManager.numBuffer - 1;
        List <File> mergedRuns = new ArrayList<>();

        int numOpen = 0;
        int runIndex = 0;
        while (runIndex < runs.size()) {
            List <TupleReader> readers = new ArrayList<>();
            while (numOpen < numBuffers && runIndex < runs.size()) {
                File current = runs.get(runIndex);
                TupleReader reader = new TupleReader(current.getName(), batchSize);
                reader.open();
                readers.add(reader);
                runIndex++;
                numOpen++;
            }
            File currentMerged = merge(readers, "mergeTmp" + mergedRuns.size());
            mergedRuns.add(currentMerged);
            numOpen = 0;
        }
        runs.forEach(r -> r.delete());
        return mergeRuns(mergedRuns);
    }

    private File merge (List<TupleReader> runs, String filename){
        if (runs.isEmpty()){
            throw new IllegalArgumentException();
        }

        File resultFile =  new File (filename);
        TupleWriter resultWriter = new TupleWriter(filename,batchSize);
        resultWriter.open();

        Tuple [] tuples = new Tuple[runs.size()];
        TupleReader [] tupleReaders = new TupleReader[runs.size()];
        for (int i = 0; i < tuples.length; i++){
            tupleReaders[i] = runs.get(i);
        }
        for (int i = 0; i < tuples.length; i++){
            tuples[i] = tupleReaders[i].next();
        }
        int numDone = 0;
        while (numDone < runs.size()){
            Tuple min = tuples[0];
            int argMin = 0;
            for (int i = 1; i < tuples.length; i++){
                Tuple current = tuples[i];
                if (tuples[i] != null){
                    if (comparator.compare(current,min) < 0) {
                        min = current;
                        argMin = i;
                    }
                }
            }
            if (min != null){
                resultWriter.next(min);
            }
            tuples [argMin] = tupleReaders[argMin].next();
            if (tupleReaders[argMin].isEOF()){
                tupleReaders[argMin].close();
                numDone ++;
            }
        }
        resultWriter.close();
        return resultFile;
    }

    private boolean generateRun (String filename) {

        int numBuffers = BufferManager.numBuffer;
        List<Tuple> runTuples = new ArrayList<>();
        int batchesRead = 0;
        Batch current;
        while (batchesRead < numBuffers && (current = base.next()) != null){
            runTuples.addAll(current.getTuples());
        }
        if (runTuples.isEmpty()){
            return false;
        }
        Collections.sort(runTuples,comparator);

        File run = new File(filename);
        sortedRuns.add(run);
        TupleWriter tupleWriter = new TupleWriter(filename,batchSize);
        tupleWriter.open();
        runTuples.forEach(t -> tupleWriter.next(t));
        tupleWriter.close();
        return true;

    }
}
