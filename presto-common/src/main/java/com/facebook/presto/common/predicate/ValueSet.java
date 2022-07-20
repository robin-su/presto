/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.common.predicate;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * 它表示的是值的集合，或者更准确的说是变量的取值范围。
 * ValueSet 表示的是取值范围，而不会把真正的所有的值都保存在里面，比如它可以表示取值是所有的int: ValueSet.all(INTEGER)
 * 但是其实它没有保存所有的int，它保存的只是描述信息而已。
 *
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EquatableValueSet.class, name = "equatable"),
        @JsonSubTypes.Type(value = SortedRangeSet.class, name = "sortable"),
        @JsonSubTypes.Type(value = AllOrNoneValueSet.class, name = "allOrNone")})
public interface ValueSet
{
    static ValueSet none(Type type)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.none(type);
        }
        if (type.isComparable()) {
            return EquatableValueSet.none(type);
        }
        return AllOrNoneValueSet.none(type);
    }

    static ValueSet all(Type type)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.all(type);
        }
        if (type.isComparable()) {
            return EquatableValueSet.all(type);
        }
        return AllOrNoneValueSet.all(type);
    }

    static ValueSet of(Type type, Object first, Object... rest)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.of(type, first, rest);
        }
        if (type.isComparable()) {
            return EquatableValueSet.of(type, first, rest);
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    static ValueSet copyOf(Type type, Collection<?> values)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.copyOf(type, values.stream()
                    .map(value -> Range.equal(type, value))
                    .collect(toList()));
        }
        if (type.isComparable()) {
            return EquatableValueSet.copyOf(type, values);
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    static ValueSet ofRanges(Range first, Range... rest)
    {
        return SortedRangeSet.of(first, rest);
    }

    static ValueSet ofRanges(List<Range> ranges)
    {
        return SortedRangeSet.of(ranges);
    }

    static ValueSet copyOfRanges(Type type, Collection<Range> ranges)
    {
        return SortedRangeSet.copyOf(type, ranges);
    }

    /**
     * 这个 ValueSet 里面值的类型
     * @return
     */
    Type getType();

    /**
     * 不匹配任何值
     * @return
     */
    boolean isNone();

    /**
     * 匹配任何值
     * @return
     */
    boolean isAll();


    boolean isSingleValue();

    Object getSingleValue();

    /**
     * 是否匹配给定的值
     * @param value
     * @return
     */
    boolean containsValue(Object value);

    /**
     * 获取这个 ValueSet 里面所有离散的值(针对不能排序的类型)
     */
    default DiscreteValues getDiscreteValues()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取这个 ValueSet 里面所有的取值范围(针对可以排序的类型)
     */
    default Ranges getRanges()
    {
        throw new UnsupportedOperationException();
    }

    ValuesProcessor getValuesProcessor();

    ValueSet intersect(ValueSet other);

    ValueSet union(ValueSet other);

    default ValueSet union(Collection<ValueSet> valueSets)
    {
        ValueSet current = this;
        for (ValueSet valueSet : valueSets) {
            current = current.union(valueSet);
        }
        return current;
    }

    ValueSet complement();

    default boolean overlaps(ValueSet other)
    {
        return !this.intersect(other).isNone();
    }

    default ValueSet subtract(ValueSet other)
    {
        return this.intersect(other.complement());
    }

    default boolean contains(ValueSet other)
    {
        return this.union(other).equals(this);
    }

    String toString(SqlFunctionProperties properties);
}
