package com.model.internal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Quartet<A, B, C, D> {
    private A a;
    private B b;
    private C c;
    private D d;

    public static <A, B, C, D> Quartet<A, B, C, D> of(A a, B b, C c, D d) {
        return new Quartet<>(a, b, c, d);
    }

    public A get1() {
        return a;
    }

    public B get2() {
        return b;
    }

    public C get3() {
        return c;
    }

    public D get4() {
        return d;
    }

    @Override
    public String toString() {
        return String.format("[%s]:[%s], [%s]:[%s], [%s]:[%s], [%s]:[%s]"
                , Objects.isNull(this.a) ? "null" : a.getClass().getSimpleName()
                , Objects.isNull(this.a) ? "null" : String.valueOf(a)
                , Objects.isNull(this.b) ? "null" : b.getClass().getSimpleName()
                , Objects.isNull(this.b) ? "null" : String.valueOf(b)
                , Objects.isNull(this.c) ? "null" : c.getClass().getSimpleName()
                , Objects.isNull(this.c) ? "null" : String.valueOf(c)
                , Objects.isNull(this.d) ? "null" : d.getClass().getSimpleName()
                , Objects.isNull(this.d) ? "null" : String.valueOf(d)
        );
    }
}