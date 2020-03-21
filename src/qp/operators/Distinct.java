package qp.operators;

public final class Distinct extends Operator {



    private final Operator base;
    public Distinct(int type , Operator base) {
        super(type);
        this.base = base;
    }



}
