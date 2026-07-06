// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Converts between Delta Lake schema types and Apache Arrow schema types.
/// </summary>
public static class SchemaConverter
{
    /// <summary>
    /// The Arrow extension name marking a Delta <c>variant</c> column's transport form: ONE self-delimiting
    /// binary value per row — the parquet-variant metadata bytes immediately followed by the value bytes
    /// (the metadata header carries its own size, so the halves split without a length prefix). The Delta
    /// type is a primitive ("variant"), but Arrow has no variant type in the C data interface, so the value
    /// crosses as this blob discriminated by the FIELD-metadata marker (Arrow types carry no metadata).
    /// Deliberately NOT the canonical arrow.parquet.variant (whose storage is struct&lt;metadata,value&gt;) —
    /// the single-blob transport is the host boundary's contract.
    /// </summary>
    public const string VariantExtensionName = "arrownet.variant";

    private const string ArrowExtensionNameKey = "ARROW:extension:name";

    /// <summary>True when the Arrow field is a variant transport struct (carries the extension marker).</summary>
    public static bool IsVariantArrowField(Field field) =>
        field.Metadata is { } md
        && md.TryGetValue(ArrowExtensionNameKey, out var ext)
        && string.Equals(ext, VariantExtensionName, StringComparison.Ordinal);

    /// <summary>The variant transport storage: one binary value per row (metadata bytes ++ value bytes).</summary>
    public static IArrowType VariantStorageType() => BinaryType.Default;

    private static readonly Regex s_decimalPattern = new(
        @"^decimal\((\d+),(\d+)\)$", RegexOptions.Compiled);

    /// <summary>
    /// Converts a Delta <see cref="StructType"/> to an Arrow <see cref="Apache.Arrow.Schema"/>.
    /// </summary>
    public static Apache.Arrow.Schema ToArrowSchema(StructType deltaSchema)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in deltaSchema.Fields)
            builder.Field(ToArrowField(field));
        return builder.Build();
    }

    /// <summary>
    /// Converts an Arrow <see cref="Apache.Arrow.Schema"/> to a Delta <see cref="StructType"/>.
    /// </summary>
    public static StructType FromArrowSchema(Apache.Arrow.Schema arrowSchema)
    {
        var fields = new List<StructField>();
        foreach (var field in arrowSchema.FieldsList)
            fields.Add(FromArrowField(field));
        return new StructType { Fields = fields };
    }

    private static Field ToArrowField(StructField field)
    {
        // variant is a Delta primitive whose Arrow transport is the unshredded parquet-variant struct,
        // discriminated by the canonical extension name in the FIELD metadata (see VariantExtensionName).
        if (field.Type is PrimitiveType pv && string.Equals(pv.TypeName, "variant", StringComparison.Ordinal))
        {
            var vmeta = new Dictionary<string, string> { [ArrowExtensionNameKey] = VariantExtensionName };
            if (field.Metadata is { Count: > 0 } vsrc)
            {
                foreach (var kvp in vsrc)
                    vmeta[kvp.Key] = kvp.Value;
            }
            return new Field(field.Name, VariantStorageType(), field.Nullable, vmeta);
        }
        var arrowType = ToArrowType(field.Type);
        // Preserve per-field Delta metadata (comments, column-mapping id/physicalName, invariants) on the
        // Arrow field — the reverse of FromArrowField's preservation, so schemas round-trip losslessly.
        Dictionary<string, string>? meta = null;
        if (field.Metadata is { Count: > 0 } src)
        {
            meta = new Dictionary<string, string>(src.Count);
            foreach (var kvp in src)
                meta[kvp.Key] = kvp.Value;
        }
        return new Field(field.Name, arrowType, field.Nullable, meta);
    }

    /// <summary>
    /// Converts a Delta <see cref="DeltaDataType"/> to an Arrow <see cref="IArrowType"/>.
    /// </summary>
    public static IArrowType ToArrowType(DeltaDataType type) => type switch
    {
        PrimitiveType p => PrimitiveToArrow(p.TypeName),
        StructType s => new ArrowStructType(
            s.Fields.Select(f => ToArrowField(f)).ToList()),
        ArrayType a => new ListType(
            new Field("element", ToArrowType(a.ElementType), a.ContainsNull)),
        MapType m => new ArrowMapType(
            new Field("key", ToArrowType(m.KeyType), false),
            new Field("value", ToArrowType(m.ValueType), m.ValueContainsNull)),
        _ => throw new DeltaLake.DeltaFormatException(
            $"Unknown Delta type: {type.GetType().Name}"),
    };

    private static IArrowType PrimitiveToArrow(string typeName)
    {
        // Check for decimal(p,s) first
        var match = s_decimalPattern.Match(typeName);
        if (match.Success)
        {
            int precision = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
            int scale = int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
            return new Decimal128Type(precision, scale);
        }

        return typeName switch
        {
            "string" => StringType.Default,
            "long" => Int64Type.Default,
            "integer" => Int32Type.Default,
            "short" => Int16Type.Default,
            "byte" => Int8Type.Default,
            "float" => FloatType.Default,
            "double" => DoubleType.Default,
            "boolean" => BooleanType.Default,
            "binary" => BinaryType.Default,
            "date" => Date32Type.Default,
            "timestamp" => new TimestampType(TimeUnit.Microsecond, (string?)"UTC"),
            "timestamp_ntz" => new TimestampType(TimeUnit.Microsecond, (string?)null),
            // variant is field-level (the marker lives in field metadata) — handled in ToArrowField for
            // top-level + struct-nested columns. A list/map ELEMENT has no field to carry the marker, so
            // that placement is rejected rather than silently degraded to a plain struct.
            "variant" => throw new DeltaLake.DeltaFormatException(
                "variant is only supported as a top-level or struct-nested column (not a list/map element)."),
            _ => throw new DeltaLake.DeltaFormatException(
                $"Unknown Delta primitive type: {typeName}"),
        };
    }

    private static StructField FromArrowField(Field field) =>
        new()
        {
            Name = field.Name,
            // The variant marker (field metadata) wins over the storage struct: without it a plain
            // struct<metadata,value> stays an ordinary struct — the marker is the only discriminator.
            Type = IsVariantArrowField(field)
                ? new PrimitiveType { TypeName = "variant" }
                : FromArrowType(field.DataType),
            Nullable = field.IsNullable,
            // Preserve per-field metadata (comments, delta.columnMapping.id/physicalName, invariants, ...) —
            // dropping it silently loses column-mapping identities on any Arrow -> Delta round-trip. Writer
            // internals (the parquet codec's "PARQUET:*" keys, e.g. PARQUET:field_id) and Arrow transport
            // markers ("ARROW:extension:*", e.g. the variant discriminator) are transport hints, not Delta
            // schema metadata — those are filtered out.
            Metadata = FilterArrowMetadata(field.Metadata),
        };

    private static Dictionary<string, string>? FilterArrowMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return null;
        Dictionary<string, string>? result = null;
        foreach (var kv in metadata)
        {
            if (kv.Key.StartsWith("PARQUET:", StringComparison.Ordinal))
                continue;
            if (kv.Key.StartsWith("ARROW:extension:", StringComparison.Ordinal))
                continue;
            (result ??= new Dictionary<string, string>())[kv.Key] = kv.Value;
        }
        return result;
    }

    private static DeltaDataType FromArrowType(IArrowType arrowType) => arrowType switch
    {
        StringType or LargeStringType or StringViewType =>
            new PrimitiveType { TypeName = "string" },
        Int64Type => new PrimitiveType { TypeName = "long" },
        Int32Type => new PrimitiveType { TypeName = "integer" },
        Int16Type => new PrimitiveType { TypeName = "short" },
        Int8Type => new PrimitiveType { TypeName = "byte" },
        FloatType => new PrimitiveType { TypeName = "float" },
        DoubleType => new PrimitiveType { TypeName = "double" },
        BooleanType => new PrimitiveType { TypeName = "boolean" },
        Decimal128Type d => new PrimitiveType
            { TypeName = $"decimal({d.Precision},{d.Scale})" },
        Decimal256Type d => new PrimitiveType
            { TypeName = $"decimal({d.Precision},{d.Scale})" },
        BinaryType or LargeBinaryType or BinaryViewType or FixedSizeBinaryType =>
            new PrimitiveType { TypeName = "binary" },
        Date32Type or Date64Type => new PrimitiveType { TypeName = "date" },
        TimestampType ts when ts.Timezone is not null =>
            new PrimitiveType { TypeName = "timestamp" },
        TimestampType => new PrimitiveType { TypeName = "timestamp_ntz" },

        ArrowStructType s => new StructType
        {
            Fields = s.Fields.Select(f => FromArrowField(f)).ToList(),
        },
        // A variant marker on a list/map INNER field would be lost through the type-level conversion below
        // (silently degrading the element to plain binary) — reject the placement instead. Struct-nested
        // variant maps fine (struct children go through FromArrowField, which sees the marker).
        ListType l when IsVariantArrowField(l.ValueField) =>
            throw new DeltaLake.DeltaFormatException(
                "variant is not supported as a list element (only top-level or struct-nested columns)."),
        ArrowMapType m0 when IsVariantArrowField(m0.KeyField) || IsVariantArrowField(m0.ValueField) =>
            throw new DeltaLake.DeltaFormatException(
                "variant is not supported as a map key/value (only top-level or struct-nested columns)."),
        ListType l => new ArrayType
        {
            ElementType = FromArrowType(l.ValueDataType),
            ContainsNull = l.ValueField.IsNullable,
        },
        ArrowMapType m => new MapType
        {
            KeyType = FromArrowType(m.KeyField.DataType),
            ValueType = FromArrowType(m.ValueField.DataType),
            ValueContainsNull = m.ValueField.IsNullable,
        },

        _ => throw new DeltaLake.DeltaFormatException(
            $"Cannot convert Arrow type {arrowType.Name} to Delta type."),
    };
}
