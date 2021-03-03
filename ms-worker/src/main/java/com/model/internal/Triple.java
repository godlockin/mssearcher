package com.model.internal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Triple<A, B, C> {
    private A a;
    private B b;
    private C c;

    public static <A, B, C> Triple<A, B, C> of(A a, B b, C c) {
        return new Triple<>(a, b, c);
    }

    public A getLeft() {
        return a;
    }

    public B getMiddle() {
        return b;
    }

    public C getRight() {
        return c;
    }

    @Override
    public String toString() {
        return String.format("[%s]:[%s], [%s]:[%s], [%s]:[%s]"
                , Objects.isNull(this.a) ? "null" : a.getClass().getSimpleName()
                , Objects.isNull(this.a) ? "null" : String.valueOf(a)
                , Objects.isNull(this.b) ? "null" : b.getClass().getSimpleName()
                , Objects.isNull(this.b) ? "null" : String.valueOf(b)
                , Objects.isNull(this.c) ? "null" : c.getClass().getSimpleName()
                , Objects.isNull(this.c) ? "null" : String.valueOf(c)
        );
    }
}