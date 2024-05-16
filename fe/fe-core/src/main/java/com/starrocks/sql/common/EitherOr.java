package com.starrocks.sql.common;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a value that can be one of two types.
 *
 * @param <L> Left type
 * @param <R> Right type
 */
public final class EitherOr<L, R> {

    private final Optional<L> left;
    private final Optional<R> right;

    public EitherOr(Optional<L> l, Optional<R> r) {
        left = l;
        right = r;
    }

    /**
     * Maps the Either to a type and returns the resolved value (which may be from the left or the right value).
     *
     * @param lFunc Function that maps the left value if present.
     * @param rFunc Function that maps the right value if present.
     * @param <T>   Type that both the left and right should be mapped to.
     * @return Mapped value from either lFunc or rFunc depending on which value is present.
     */
    public <T> T map(Function<? super L, ? extends T> lFunc,
                     Function<? super R, ? extends T> rFunc) {
        return left.<T>map(lFunc).orElseGet(() -> right.map(rFunc).get());
    }

    /**
     * Map the left most value and return a new Either reflecting the new types.
     *
     * @param lFunc Function that maps the left value if present.
     * @param <T>   New type of left value.
     * @return New Either bound to the new left type and the same right type.
     */
    public <T> EitherOr<T, R> mapLeft(Function<? super L, ? extends T> lFunc) {
        return new EitherOr<>(left.map(lFunc), right);
    }

    /**
     * Map the right most value and return a new Either reflecting the new types.
     *
     * @param rFunc Function that maps the right value if present.
     * @param <T>   New type of right value.
     * @return New Either bound to the same left type and the new right type.
     */
    public <T> EitherOr<L, T> mapRight(Function<? super R, ? extends T> rFunc) {
        return new EitherOr<>(left, right.map(rFunc));
    }

    /**
     * Apply the consumers to the left or the right value depending on which is present.
     *
     * @param lFunc Consumer of left value, invoked if left value is present.
     * @param rFunc Consumer of right value, invoked if right value is present.
     */
    public void apply(Consumer<? super L> lFunc, Consumer<? super R> rFunc) {
        left.ifPresent(lFunc);
        right.ifPresent(rFunc);
    }

    /**
     * Create a new Either with the left type.
     *
     * @param value Left value
     * @param <L>   Left type
     * @param <R>   Right type
     */
    public static <L, R> EitherOr<L, R> left(L value) {
        return new EitherOr<>(Optional.of(value), Optional.empty());
    }

    /**
     * Create a new Either with the right type.
     *
     * @param value Right value
     * @param <L>   Left type
     * @param <R>   Right type
     */
    public static <L, R> EitherOr<L, R> right(R value) {
        return new EitherOr<>(Optional.empty(), Optional.of(value));
    }

    /**
     * @return the left value
     */
    public Optional<L> left() {
        return left;
    }

    /**
     * @return the right value
     */
    public Optional<R> right() {
        return right;
    }

    /**
     * Create a new {@code Optional<Either>} from two possibly null values.
     *
     * If both values are null, {@link Optional#empty()} is returned. Only one of the left or right values
     * is allowed to be non-null, otherwise an {@link IllegalArgumentException} is thrown.
     * @param left The left value (possibly null)
     * @param right The right value (possibly null)
     * @param <L> Left type
     * @param <R> Right type
     * @return an Optional Either representing one of the two values or empty if both are null
     */
    public static <L, R> Optional<EitherOr<L, R>> fromNullable(L left, R right) {
        if (left != null && right == null) {
            return Optional.of(left(left));
        }
        if (left == null && right != null) {
            return Optional.of(right(right));
        }
        if (left == null && right == null) {
            return Optional.empty();
        }
        throw new IllegalArgumentException(String.format("Only one of either left or right should be non-null. "
                + "Got (left: %s, right: %s)", left, right));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EitherOr)) {
            return false;
        }

        EitherOr<?, ?> either = (EitherOr<?, ?>) o;

        return left.equals(either.left) && right.equals(either.right);
    }

    @Override
    public int hashCode() {
        return 31 * left.hashCode() + right.hashCode();
    }
}