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
        return super.open();
    }

    @Override
    public boolean close() {
        return super.close();
    }

    @Override
    public Batch next() {
        return super.next();
    }
}
