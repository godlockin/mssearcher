package com.model.internal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pair<A, B> {
    private A a;
    private B b;

    public static <A, B> Pair<A, B> of(A a, B b) {
        return new Pair<>(a, b);
    }

    public A getKey() {
        return a;
    }

    public B getValue() {
        return b;
    }

    @Override
    public String toString() {
        return String.format("[%s]:[%s], [%s]:[%s]"
                , Objects.isNull(this.a) ? "null" : a.getClass().getSimpleName()
                , Objects.isNull(this.a) ? "null" : String.valueOf(a)
                , Objects.isNull(this.b) ? "null" : b.getClass().getSimpleName()
                , Objects.isNull(this.b) ? "null" : String.valueOf(b)
        );
    }
}
