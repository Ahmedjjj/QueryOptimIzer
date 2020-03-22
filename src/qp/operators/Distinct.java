package qp.operators;

import qp.utils.Batch;

public final class Distinct extends Operator {

    private final Operator base;
    private Operator sortedBase;
    public Distinct(int type , Operator base) {
        super(type);
        this.base = base;
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
