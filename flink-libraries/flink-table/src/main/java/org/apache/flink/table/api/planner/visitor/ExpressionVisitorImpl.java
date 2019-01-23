/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.planner.visitor;

import org.apache.flink.table.api.base.visitor.ExpressionVisitor;
import org.apache.flink.table.api.planner.converters.rex.CastRexConverter;
import org.apache.flink.table.expressions.Abs;
import org.apache.flink.table.expressions.Acos;
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.Alias;
import org.apache.flink.table.expressions.And;
import org.apache.flink.table.expressions.ArrayConstructor;
import org.apache.flink.table.expressions.ArrayElement;
import org.apache.flink.table.expressions.Asc;
import org.apache.flink.table.expressions.Asin;
import org.apache.flink.table.expressions.Atan;
import org.apache.flink.table.expressions.Atan2;
import org.apache.flink.table.expressions.Between;
import org.apache.flink.table.expressions.Bin;
import org.apache.flink.table.expressions.Cardinality;
import org.apache.flink.table.expressions.Cast;
import org.apache.flink.table.expressions.Ceil;
import org.apache.flink.table.expressions.CharLength;
import org.apache.flink.table.expressions.Concat;
import org.apache.flink.table.expressions.ConcatWs;
import org.apache.flink.table.expressions.Cos;
import org.apache.flink.table.expressions.Cosh;
import org.apache.flink.table.expressions.Cot;
import org.apache.flink.table.expressions.CurrentDate;
import org.apache.flink.table.expressions.CurrentTime;
import org.apache.flink.table.expressions.CurrentTimestamp;
import org.apache.flink.table.expressions.DateFormat;
import org.apache.flink.table.expressions.Degrees;
import org.apache.flink.table.expressions.Desc;
import org.apache.flink.table.expressions.Div;
import org.apache.flink.table.expressions.E;
import org.apache.flink.table.expressions.EqualTo;
import org.apache.flink.table.expressions.Exp;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Extract;
import org.apache.flink.table.expressions.Floor;
import org.apache.flink.table.expressions.FromBase64;
import org.apache.flink.table.expressions.GetCompositeField;
import org.apache.flink.table.expressions.GreaterThan;
import org.apache.flink.table.expressions.GreaterThanOrEqual;
import org.apache.flink.table.expressions.Hex;
import org.apache.flink.table.expressions.If;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.expressions.InitCap;
import org.apache.flink.table.expressions.IsFalse;
import org.apache.flink.table.expressions.IsNotFalse;
import org.apache.flink.table.expressions.IsNotNull;
import org.apache.flink.table.expressions.IsNotTrue;
import org.apache.flink.table.expressions.IsNull;
import org.apache.flink.table.expressions.IsTrue;
import org.apache.flink.table.expressions.ItemAt;
import org.apache.flink.table.expressions.LTrim;
import org.apache.flink.table.expressions.LessThan;
import org.apache.flink.table.expressions.LessThanOrEqual;
import org.apache.flink.table.expressions.Like;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.Ln;
import org.apache.flink.table.expressions.LocalTime;
import org.apache.flink.table.expressions.LocalTimestamp;
import org.apache.flink.table.expressions.Log;
import org.apache.flink.table.expressions.Log10;
import org.apache.flink.table.expressions.Log2;
import org.apache.flink.table.expressions.Lower;
import org.apache.flink.table.expressions.Lpad;
import org.apache.flink.table.expressions.MapConstructor;
import org.apache.flink.table.expressions.Md5;
import org.apache.flink.table.expressions.Minus;
import org.apache.flink.table.expressions.Mod;
import org.apache.flink.table.expressions.Mul;
import org.apache.flink.table.expressions.Not;
import org.apache.flink.table.expressions.NotBetween;
import org.apache.flink.table.expressions.NotEqualTo;
import org.apache.flink.table.expressions.Null;
import org.apache.flink.table.expressions.Or;
import org.apache.flink.table.expressions.OverCall;
import org.apache.flink.table.expressions.Overlay;
import org.apache.flink.table.expressions.Pi;
import org.apache.flink.table.expressions.Plus;
import org.apache.flink.table.expressions.Position;
import org.apache.flink.table.expressions.Power;
import org.apache.flink.table.expressions.Quarter;
import org.apache.flink.table.expressions.RTrim;
import org.apache.flink.table.expressions.Radians;
import org.apache.flink.table.expressions.Rand;
import org.apache.flink.table.expressions.RandInteger;
import org.apache.flink.table.expressions.RegexpExtract;
import org.apache.flink.table.expressions.RegexpReplace;
import org.apache.flink.table.expressions.Repeat;
import org.apache.flink.table.expressions.Replace;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.expressions.Round;
import org.apache.flink.table.expressions.RowConstructor;
import org.apache.flink.table.expressions.Rpad;
import org.apache.flink.table.expressions.ScalarFunctionCall;
import org.apache.flink.table.expressions.Sha1;
import org.apache.flink.table.expressions.Sha2;
import org.apache.flink.table.expressions.Sha224;
import org.apache.flink.table.expressions.Sha256;
import org.apache.flink.table.expressions.Sha384;
import org.apache.flink.table.expressions.Sha512;
import org.apache.flink.table.expressions.Sign;
import org.apache.flink.table.expressions.Similar;
import org.apache.flink.table.expressions.Sin;
import org.apache.flink.table.expressions.Sinh;
import org.apache.flink.table.expressions.Sqrt;
import org.apache.flink.table.expressions.StreamRecordTimestamp;
import org.apache.flink.table.expressions.Substring;
import org.apache.flink.table.expressions.SymbolExpression;
import org.apache.flink.table.expressions.Tan;
import org.apache.flink.table.expressions.Tanh;
import org.apache.flink.table.expressions.TemporalCeil;
import org.apache.flink.table.expressions.TemporalFloor;
import org.apache.flink.table.expressions.TemporalOverlaps;
import org.apache.flink.table.expressions.TimestampDiff;
import org.apache.flink.table.expressions.ToBase64;
import org.apache.flink.table.expressions.Trim;
import org.apache.flink.table.expressions.UUID;
import org.apache.flink.table.expressions.UnaryMinus;
import org.apache.flink.table.expressions.Upper;
import org.apache.flink.table.plan.logical.Join;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * RexNodeVisitorImpl.
 */
public class ExpressionVisitorImpl implements ExpressionVisitor<RexNode> {
	public RelBuilder relBuilder;

	public ExpressionVisitorImpl(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	public static RexNode toRexNode(Expression expr, RelBuilder relBuilder) {
		return new ExpressionVisitorImpl(relBuilder).toRexNode(expr);
	}

	public RexNode toRexNode(Expression expr) {
		return expr.accept(this);
	}

	@Override
	public RexNode visit(AggFunctionCall aggFunctionCall) {
		return null;
	}

	@Override
	public RexNode visit(Plus plus) {
		return null;
	}

	@Override
	public RexNode visit(UnaryMinus unaryMinus) {
		return null;
	}

	@Override
	public RexNode visit(Minus minus) {
		return null;
	}

	@Override
	public RexNode visit(Div div) {
		return null;
	}

	@Override
	public RexNode visit(Mul mul) {
		return null;
	}

	@Override
	public RexNode visit(Mod mod) {
		return null;
	}

	@Override
	public RexNode visit(OverCall overCall) {
		return null;
	}

	@Override
	public RexNode visit(ScalarFunctionCall scalarFunctionCall) {
		return null;
	}

	@Override
	public RexNode visit(Cast cast) {
		return CastRexConverter.toRexNode(cast, this);
	}

	@Override
	public RexNode visit(RowConstructor rowConstructor) {
		return null;
	}

	@Override
	public RexNode visit(ArrayConstructor arrayConstructor) {
		return null;
	}

	@Override
	public RexNode visit(MapConstructor mapConstructor) {
		return null;
	}

	@Override
	public RexNode visit(ArrayElement arrayElement) {
		return null;
	}

	@Override
	public RexNode visit(Cardinality cardinality) {
		return null;
	}

	@Override
	public RexNode visit(ItemAt itemAt) {
		return null;
	}

	@Override
	public RexNode visit(EqualTo equalTo) {
		return null;
	}

	@Override
	public RexNode visit(NotEqualTo notEqualTo) {
		return null;
	}

	@Override
	public RexNode visit(GreaterThan greaterThan) {
		return null;
	}

	@Override
	public RexNode visit(GreaterThanOrEqual greaterThanOrEqual) {
		return null;
	}

	@Override
	public RexNode visit(LessThan lessThan) {
		return null;
	}

	@Override
	public RexNode visit(LessThanOrEqual lessThanOrEqual) {
		return null;
	}

	@Override
	public RexNode visit(IsNull isNull) {
		return null;
	}

	@Override
	public RexNode visit(IsNotNull isNotNull) {
		return null;
	}

	@Override
	public RexNode visit(IsTrue isTrue) {
		return null;
	}

	@Override
	public RexNode visit(IsFalse isFalse) {
		return null;
	}

	@Override
	public RexNode visit(IsNotTrue isNotTrue) {
		return null;
	}

	@Override
	public RexNode visit(IsNotFalse isNotFalse) {
		return null;
	}

	@Override
	public RexNode visit(Between between) {
		return null;
	}

	@Override
	public RexNode visit(NotBetween notBetween) {
		return null;
	}

	@Override
	public RexNode visit(GetCompositeField getCompositeField) {
		return null;
	}

	@Override
	public RexNode visit(ResolvedFieldReference resolvedFieldReference) {
		return null;
	}

	@Override
	public RexNode visit(Alias alias) {
		return null;
	}

	@Override
	public RexNode visit(StreamRecordTimestamp streamRecordTimestamp) {
		return null;
	}

	@Override
	public RexNode visit(Md5 md5) {
		return null;
	}

	@Override
	public RexNode visit(Sha1 sha1) {
		return null;
	}

	@Override
	public RexNode visit(Sha224 sha224) {
		return null;
	}

	@Override
	public RexNode visit(Sha256 sha256) {
		return null;
	}

	@Override
	public RexNode visit(Sha384 sha384) {
		return null;
	}

	@Override
	public RexNode visit(Sha512 sha512) {
		return null;
	}

	@Override
	public RexNode visit(Sha2 sha2) {
		return null;
	}

	@Override
	public RexNode visit(Literal literal) {
		return null;
	}

	@Override
	public RexNode visit(Null nullExpr) {
		return null;
	}

	@Override
	public RexNode visit(Not not) {
		return null;
	}

	@Override
	public RexNode visit(And and) {
		return null;
	}

	@Override
	public RexNode visit(Or or) {
		return null;
	}

	@Override
	public RexNode visit(If ifExpr) {
		return null;
	}

	@Override
	public RexNode visit(Abs abs) {
		return null;
	}

	@Override
	public RexNode visit(Ceil ceil) {
		return null;
	}

	@Override
	public RexNode visit(Exp exp) {
		return null;
	}

	@Override
	public RexNode visit(Floor floor) {
		return null;
	}

	@Override
	public RexNode visit(Log10 log10) {
		return null;
	}

	@Override
	public RexNode visit(Log2 log2) {
		return null;
	}

	@Override
	public RexNode visit(Cosh cosh) {
		return null;
	}

	@Override
	public RexNode visit(Log log) {
		return null;
	}

	@Override
	public RexNode visit(Ln ln) {
		return null;
	}

	@Override
	public RexNode visit(Power power) {
		return null;
	}

	@Override
	public RexNode visit(Sinh sinh) {
		return null;
	}

	@Override
	public RexNode visit(Sqrt sqrt) {
		return null;
	}

	@Override
	public RexNode visit(Sin sin) {
		return null;
	}

	@Override
	public RexNode visit(Cos cos) {
		return null;
	}

	@Override
	public RexNode visit(Tan tan) {
		return null;
	}

	@Override
	public RexNode visit(Tanh tanh) {
		return null;
	}

	@Override
	public RexNode visit(Cot cot) {
		return null;
	}

	@Override
	public RexNode visit(Asin asin) {
		return null;
	}

	@Override
	public RexNode visit(Acos acos) {
		return null;
	}

	@Override
	public RexNode visit(Atan atan) {
		return null;
	}

	@Override
	public RexNode visit(Atan2 atan2) {
		return null;
	}

	@Override
	public RexNode visit(Degrees degrees) {
		return null;
	}

	@Override
	public RexNode visit(Radians radians) {
		return null;
	}

	@Override
	public RexNode visit(Sign sign) {
		return null;
	}

	@Override
	public RexNode visit(Round round) {
		return null;
	}

	@Override
	public RexNode visit(Pi pi) {
		return null;
	}

	@Override
	public RexNode visit(E e) {
		return null;
	}

	@Override
	public RexNode visit(Rand rand) {
		return null;
	}

	@Override
	public RexNode visit(RandInteger randInteger) {
		return null;
	}

	@Override
	public RexNode visit(Bin bin) {
		return null;
	}

	@Override
	public RexNode visit(Hex hex) {
		return null;
	}

	@Override
	public RexNode visit(UUID uuid) {
		return null;
	}

	@Override
	public RexNode visit(Asc asc) {
		return null;
	}

	@Override
	public RexNode visit(Desc desc) {
		return null;
	}

	@Override
	public RexNode visit(CharLength charLength) {
		return null;
	}

	@Override
	public RexNode visit(InitCap initCap) {
		return null;
	}

	@Override
	public RexNode visit(Like like) {
		return null;
	}

	@Override
	public RexNode visit(Lower lower) {
		return null;
	}

	@Override
	public RexNode visit(Similar similar) {
		return null;
	}

	@Override
	public RexNode visit(Substring substring) {
		return null;
	}

	@Override
	public RexNode visit(Trim trim) {
		return null;
	}

	@Override
	public RexNode visit(Upper upper) {
		return null;
	}

	@Override
	public RexNode visit(Position position) {
		return null;
	}

	@Override
	public RexNode visit(Overlay overlay) {
		return null;
	}

	@Override
	public RexNode visit(Concat concat) {
		return null;
	}

	@Override
	public RexNode visit(ConcatWs concatWs) {
		return null;
	}

	@Override
	public RexNode visit(Lpad lpad) {
		return null;
	}

	@Override
	public RexNode visit(Rpad rpad) {
		return null;
	}

	@Override
	public RexNode visit(RegexpReplace regexpReplace) {
		return null;
	}

	@Override
	public RexNode visit(RegexpExtract regexpExtract) {
		return null;
	}

	@Override
	public RexNode visit(FromBase64 fromBase64) {
		return null;
	}

	@Override
	public RexNode visit(ToBase64 toBase64) {
		return null;
	}

	@Override
	public RexNode visit(LTrim lTrim) {
		return null;
	}

	@Override
	public RexNode visit(RTrim rTrim) {
		return null;
	}

	@Override
	public RexNode visit(Repeat repeat) {
		return null;
	}

	@Override
	public RexNode visit(Replace replace) {
		return null;
	}

	@Override
	public RexNode visit(In in) {
		return null;
	}

	@Override
	public RexNode visit(SymbolExpression symbolExpression) {
		return null;
	}

	@Override
	public RexNode visit(Extract extract) {
		return null;
	}

	@Override
	public RexNode visit(TemporalFloor temporalFloor) {
		return null;
	}

	@Override
	public RexNode visit(TemporalCeil temporalCeil) {
		return null;
	}

	@Override
	public RexNode visit(CurrentDate currentDate) {
		return null;
	}

	@Override
	public RexNode visit(CurrentTime currentTime) {
		return null;
	}

	@Override
	public RexNode visit(CurrentTimestamp currentTimestamp) {
		return null;
	}

	@Override
	public RexNode visit(LocalTime localTime) {
		return null;
	}

	@Override
	public RexNode visit(LocalTimestamp localTimestamp) {
		return null;
	}

	@Override
	public RexNode visit(Quarter quarter) {
		return null;
	}

	@Override
	public RexNode visit(TemporalOverlaps temporalOverlaps) {
		return null;
	}

	@Override
	public RexNode visit(DateFormat dateFormat) {
		return null;
	}

	@Override
	public RexNode visit(TimestampDiff timestampDiff) {
		return null;
	}

	@Override
	public RexNode visit(Join.JoinFieldReference joinFieldReference) {
		return null;
	}
}
