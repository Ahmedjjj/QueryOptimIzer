package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;

public final class Distinct extends Operator {

    private Operator base;
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

    public void setBase (Operator base) {this.base = base;}
    @Override
    public Batch next() {
        return sortedBase.next();
    }

    public Object clone() {
        Distinct newDistinct = new Distinct((Operator)base.clone());
        newDistinct.setSchema((Schema)this.getSchema().clone());
        return newDistinct;
    }
}
