package qp.operators;

import qp.utils.Batch;

public final class Distinct extends Operator {

    private final Operator base;
    private Operator sortedBase;
    public Distinct(Operator base) {
        super(OpType.DISTINCT);
        this.base = base;
    }

    public Operator getBase() {
        return this.base;
    }

    @Override
    public boolean open() {
        sortedBase = new Sort(base.getSchema().getAttList(),base,true);
        sortedBase.open();
        return true;
    }

    @Override
    public boolean close() {
        sortedBase.close();
        base.close();
        return true;
    }

    @Override
    public Batch next() {
        return sortedBase.next();
    }
}
