// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Lowers `_metadata` predicates onto a <see cref="FileRowSelection"/> — the symbolic evaluation that
/// makes a metadata DELETE/UPDATE run without scanning: <c>_metadata.file_path</c> equality selects files
/// by identity (no read, no stats), <c>_metadata.row_index</c> IN/equality names the positions. Supported
/// shape: <c>(file_path = 'f' AND row_index IN (…))</c> — the two conjuncts in either order, a single
/// <c>row_index = k</c> also accepted — OR-combined for several files (same file twice unions). Anything
/// else does not lower; a predicate that references `_metadata` but cannot lower is REJECTED rather than
/// silently mis-evaluated by the row mask (which only sees data columns).
/// </summary>
public static class MetadataPredicate
{
    public const string FilePathColumn = "_metadata.file_path";
    public const string RowIndexColumn = "_metadata.row_index";

    /// <summary>Attempts the symbolic lowering. False = the shape is not (purely) a metadata selection.</summary>
    public static bool TryLower(Predicate predicate, out FileRowSelection selection)
    {
        selection = null!;
        var files = new Dictionary<string, HashSet<long>>(StringComparer.Ordinal);
        if (!TryCollect(predicate, files) || files.Count == 0)
            return false;
        selection = new FileRowSelection(files.ToDictionary(
            kv => kv.Key, kv => (IReadOnlyCollection<long>)kv.Value.ToList(), StringComparer.Ordinal));
        return true;
    }

    /// <summary>Guards the mask fallback: a predicate that mentions `_metadata.*` but did not lower must
    /// error loudly — the row evaluator binds data columns only and would mis-evaluate it.</summary>
    public static void ThrowIfReferencesMetadata(Predicate predicate, string operation)
    {
        if (ReferencesMetadata(predicate))
        {
            throw new NotSupportedException(
                $"{operation}: the predicate references `_metadata.*` in a shape that cannot be lowered to "
                + "a file/row selection. Supported: (_metadata.file_path = '<add.path>' AND "
                + "_metadata.row_index IN (…)), OR-combined per file. Mixed metadata+data predicates are "
                + "not supported yet.");
        }
    }

    private static bool TryCollect(Predicate p, Dictionary<string, HashSet<long>> files) => p switch
    {
        OrPredicate or_ => or_.Children.Count > 0 && or_.Children.All(c => TryCollect(c, files)),
        AndPredicate and => TryLowerConjunction(and, files),
        _ => false,
    };

    private static bool TryLowerConjunction(AndPredicate and, Dictionary<string, HashSet<long>> files)
    {
        if (and.Children.Count != 2)
            return false;

        string? path = null;
        List<long>? rows = null;
        foreach (var child in and.Children)
        {
            if (TryFilePathEquals(child, out var p2))
            {
                if (path is not null)
                    return false;
                path = p2;
            }
            else if (TryRowIndexSet(child, out var r))
            {
                if (rows is not null)
                    return false;
                rows = r;
            }
            else
            {
                return false;
            }
        }

        if (path is null || rows is null)
            return false;

        if (!files.TryGetValue(path, out var set))
            files[path] = set = new HashSet<long>();
        foreach (long r in rows)
            set.Add(r);
        return true;
    }

    private static bool TryFilePathEquals(Predicate p, out string path)
    {
        path = "";
        if (p is not ComparisonPredicate { Op: ComparisonOperator.Equal } c)
            return false;
        var (refSide, litSide) = (c.Left, c.Right);
        if (!IsRef(refSide, FilePathColumn))
            (refSide, litSide) = (c.Right, c.Left);
        if (!IsRef(refSide, FilePathColumn) || litSide is not LiteralExpression le
            || le.Value.ToObject() is not string s)
        {
            return false;
        }
        path = s;
        return true;
    }

    private static bool TryRowIndexSet(Predicate p, out List<long> rows)
    {
        rows = null!;
        switch (p)
        {
            case SetPredicate { Op: SetOperator.In } s when IsRef(s.Operand, RowIndexColumn):
            {
                var result = new List<long>(s.Values.Count);
                foreach (var v in s.Values)
                {
                    if (!TryToInt64(v.ToObject(), out long l))
                        return false;
                    result.Add(l);
                }
                rows = result;
                return true;
            }
            case ComparisonPredicate { Op: ComparisonOperator.Equal } c:
            {
                var (refSide, litSide) = (c.Left, c.Right);
                if (!IsRef(refSide, RowIndexColumn))
                    (refSide, litSide) = (c.Right, c.Left);
                if (!IsRef(refSide, RowIndexColumn) || litSide is not LiteralExpression le
                    || !TryToInt64(le.Value.ToObject(), out long l))
                {
                    return false;
                }
                rows = new List<long> { l };
                return true;
            }
            default:
                return false;
        }
    }

    private static bool TryToInt64(object? o, out long value)
    {
        switch (o)
        {
            case long l: value = l; return true;
            case int i: value = i; return true;
            case short s: value = s; return true;
            default: value = 0; return false;
        }
    }

    private static bool IsRef(Expression e, string name) => e switch
    {
        UnboundReference u => string.Equals(u.Name, name, StringComparison.Ordinal),
        BoundReference b => string.Equals(b.Name, name, StringComparison.Ordinal),
        _ => false,
    };

    private static bool ReferencesMetadata(Predicate p) => p switch
    {
        AndPredicate and => and.Children.Any(ReferencesMetadata),
        OrPredicate or_ => or_.Children.Any(ReferencesMetadata),
        NotPredicate not => ReferencesMetadata(not.Child),
        ComparisonPredicate c => ExprReferencesMetadata(c.Left) || ExprReferencesMetadata(c.Right),
        UnaryPredicate u => ExprReferencesMetadata(u.Operand),
        SetPredicate s => ExprReferencesMetadata(s.Operand),
        _ => false,
    };

    private static bool ExprReferencesMetadata(Expression e) => e switch
    {
        UnboundReference u => u.Name.StartsWith("_metadata", StringComparison.Ordinal),
        BoundReference b => b.Name.StartsWith("_metadata", StringComparison.Ordinal),
        _ => false,
    };
}
