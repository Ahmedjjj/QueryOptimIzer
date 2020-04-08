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
    private final boolean distinct;

    private List<File> sortedRuns = new ArrayList<>();
    private ObjectInputStream batchInputStream;
    private File sortedFile;

    public Sort (List<Attribute> sortAttributes, Operator base, boolean distinct){
        super(OpType.SORT);
        this.base = base;
        this.sortAttributes = sortAttributes;
        this.comparator = getComparator();
        this.attributeIndexes = getAttributeIndexes();
        this.batchSize = Batch.getPageSize() / base.getSchema().getTupleSize();
        this.distinct = distinct;
    }

    public Operator getBase(){
        return base;
    }

    @Override
    public boolean open() {
        base.open();
        File file = sort();
        try {
            batchInputStream = new ObjectInputStream(new FileInputStream(file.getName()));
            sortedFile = file;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            sortedFile.delete();
            base.close();
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
        } catch (EOFException e) {
            return null;
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
        if (runs == null) {
            throw new NullPointerException("Runs cannot be null");
        } else if (runs.isEmpty()){
            throw new IllegalArgumentException("List of runs to merge is empty");
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
            File currentMerged = merge(readers, "mergeTmp");
            mergedRuns.add(currentMerged);
            numOpen = 0;
        }
        runs.forEach(r -> r.delete());
        return mergeRuns(mergedRuns);
    }
    private int runCounter = 0;
    private File merge (List<TupleReader> runs, String filename){
        if (runs.isEmpty()){
            throw new IllegalArgumentException();
        }

        File resultFile =  new File (filename + runCounter);
        runCounter++;
        TupleWriter resultWriter = new TupleWriter(resultFile.getName(),batchSize);
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
            Tuple previousMin = null;
            Tuple min = tuples[0];
            int argMin = 0;
            for (int i = 1; i < tuples.length; i++){
                Tuple current = tuples[i];
                if (min == null){
                    min = current;
                    argMin = i;
                }else if (tuples[i] != null){
                    if (comparator.compare(current,min) < 0) {
                        min = current;
                        argMin = i;
                    }
                }
            }
            if (min != null && (!distinct || previousMin == null || comparator.compare(previousMin,min) != 0)){
                resultWriter.next(min);
                previousMin = min;
            }
            tuples [argMin] = tupleReaders[argMin].next();
            if (tuples[argMin] == null){
                tupleReaders[argMin].close();
                numDone++;
            }
        }
        resultWriter.close();
        return resultFile;
    }

    private boolean generateRun (String filename) {

        int numBuffers = BufferManager.numBuffer;

        Collection<Tuple> runTuples = distinct ? new TreeSet(comparator) : new ArrayList<>();

        int batchesRead = 0;
        Batch current;

        while (batchesRead < numBuffers && (current = base.next()) != null){
            runTuples.addAll(current.getTuples());
            batchesRead++;
        }
        if (runTuples.isEmpty()){
            return false;
        }
        if (!distinct) {
            Collections.sort((List<Tuple>) runTuples, comparator);
        }
        File run = new File(filename);
        this.sortedRuns.add(run);
        TupleWriter tupleWriter = new TupleWriter(filename,batchSize);
        tupleWriter.open();
        runTuples.forEach(t -> tupleWriter.next(t));
        tupleWriter.close();
        return true;

    }
}
